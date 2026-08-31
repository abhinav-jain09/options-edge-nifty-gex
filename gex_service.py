#!/usr/bin/env python3
"""NIFTY/BANKNIFTY Pressure (gamma-exposure) boards backed by the DhanHQ v2 API.

ONE process serves every configured underlying: Dhan's option-chain limit
(1 unique request / 3 s) and the quote limit (1 / s) are PER ACCOUNT, shared
across underlyings — so boards poll the chain round-robin on a single budget,
spot for all of them arrives in ONE batched /marketfeed/ltp call, and a single
renew loop owns the 24h token. Never run two copies against one account.

Routes (per board key, e.g. nifty-gex, banknifty-gex):
  GET /<key>/        - HTML board       GET /<key>/gex     - JSON snapshot
  GET /<key>/health  - health           GET /              - first board's page

Convention: call GEX positive, put GEX negative (dealers long calls / short puts).
GEX per strike = gamma * OI * OI_MULTIPLIER * spot^2 * 0.01 (₹ per 1% move, shown
in crore). Dhan's `oi` is in underlying units (lot included) so the multiplier
defaults to 1.
"""

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API_BASE = "https://api.dhan.co/v2"

ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")  # rotated in place by _renew_loop
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
# Dhan tokens live 24h. GET /v2/RenewToken (the docs say POST — that 400s with DH-905;
# GET is what actually works) returns {createTime, expiryTime, token} and EXPIRES the
# old token. Renew while the token is still active: an expired token cannot renew itself.
RENEW_BEFORE_HOURS = float(os.environ.get("DHAN_GEX_RENEW_BEFORE_HOURS", "12"))
SECRET_NAME = os.environ.get("DHAN_GEX_SECRET_NAME", "dhan-credentials")  # empty = no writeback
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

# Boards: comma-separated  path-key:NAME:scrip:segment.  The path key doubles as the
# public URL prefix (a cloudflared path route must exist for each exposed key).
BOARDS_SPEC = os.environ.get(
    "DHAN_GEX_BOARDS", "nifty-gex:NIFTY:13:IDX_I,banknifty-gex:BANKNIFTY:25:IDX_I")
FIXED_EXPIRY = os.environ.get("DHAN_GEX_EXPIRY", "")  # YYYY-MM-DD, applies to ALL boards
POLL_SECONDS = float(os.environ.get("DHAN_GEX_POLL_SECONDS", "3.5"))
SPOT_POLL_SECONDS = float(os.environ.get("DHAN_GEX_SPOT_POLL_SECONDS", "1.1"))
SPOT_FRESH_SECONDS = 15.0  # fall back to the chain's own last_price beyond this
OI_MULTIPLIER = float(os.environ.get("DHAN_GEX_OI_MULTIPLIER", "1"))
PORT = int(os.environ.get("DHAN_GEX_PORT", "8188"))
BIND = os.environ.get("DHAN_GEX_BIND", "127.0.0.1")
WALL_BAND_PCT = float(os.environ.get("DHAN_GEX_WALL_BAND_PCT", "5"))

CRORE = 1e7

_lock = threading.Lock()
_boards = []   # [{key,name,scrip,seg, chain,snapshot,expiries,expiries_on,error}]
_spots = {}    # scrip -> (price, monotonic-ts)
_spot_error = None
_auth_failed = False  # a poller saw 401 — renew loop reacts on its next tick
_renewed_at = None
_renew_error = None
_secret_written_at = None
_secret_error = None


def _parse_boards():
    boards = []
    for part in BOARDS_SPEC.split(","):
        key, name, scrip, seg = [p.strip() for p in part.strip().split(":")]
        boards.append({"key": key.strip("/"), "name": name, "scrip": int(scrip), "seg": seg,
                       "chain": None, "snapshot": None, "expiries": [], "expiries_on": None,
                       "error": None})
    return boards


def _api_post(path, body):
    req = urllib.request.Request(
        API_BASE + path,
        data=json.dumps(body).encode(),
        headers={
            "access-token": ACCESS_TOKEN,
            "client-id": CLIENT_ID,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode())
    if payload.get("status") != "success":
        raise RuntimeError(f"API status={payload.get('status')}: {str(payload)[:200]}")
    return payload["data"]


def _jwt_exp(token):
    """exp claim (unix seconds) of a JWT, 0 if undecodable."""
    import base64
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return float(claims.get("exp", 0))
    except Exception:
        return 0.0


def _write_secret(token):
    """Persist the renewed token into the k8s Secret so a pod restart survives it.
    No-op outside a cluster or with SECRET_NAME empty."""
    global _secret_written_at, _secret_error
    if not SECRET_NAME or not os.path.exists(os.path.join(SA_DIR, "token")):
        return
    import base64
    import ssl
    try:
        sa_token = open(os.path.join(SA_DIR, "token")).read().strip()
        namespace = open(os.path.join(SA_DIR, "namespace")).read().strip()
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        url = f"https://{host}:{port}/api/v1/namespaces/{namespace}/secrets/{SECRET_NAME}"
        body = json.dumps({"data": {"DHAN_ACCESS_TOKEN": base64.b64encode(token.encode()).decode()}})
        req = urllib.request.Request(url, data=body.encode(), method="PATCH", headers={
            "Authorization": "Bearer " + sa_token,
            "Content-Type": "application/strategic-merge-patch+json",
        })
        ctx = ssl.create_default_context(cafile=os.path.join(SA_DIR, "ca.crt"))
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            resp.read()
        _secret_written_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        _secret_error = None
    except Exception as e:
        _secret_error = f"{type(e).__name__}: {e}"


def _renew_loop():
    """Keep the 24h Dhan token alive: renew whenever less than RENEW_BEFORE_HOURS remain
    (or immediately after a poller hits 401, in case the clock-based check was beaten)."""
    global ACCESS_TOKEN, _auth_failed, _renewed_at, _renew_error
    while True:
        time.sleep(60)
        try:
            left = _jwt_exp(ACCESS_TOKEN) - time.time()
            if left > RENEW_BEFORE_HOURS * 3600 and not _auth_failed:
                continue
            req = urllib.request.Request(
                API_BASE + "/RenewToken", method="GET",
                headers={"access-token": ACCESS_TOKEN, "dhanClientId": CLIENT_ID})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            new_token = data.get("token", "")
            if not new_token:
                raise RuntimeError(f"no token in response: {str(data)[:120]}")
            ACCESS_TOKEN = new_token
            _auth_failed = False
            _renewed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            _renew_error = None
            print(f"token renewed, new expiry {data.get('expiryTime')}", flush=True)
            _write_secret(new_token)
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode()[:150]
            except Exception:
                pass
            _renew_error = f"HTTP {e.code}: {body}"
        except Exception as e:
            _renew_error = f"{type(e).__name__}: {e}"


def _active_expiry(board):
    if FIXED_EXPIRY:
        return FIXED_EXPIRY
    today = date.today().isoformat()
    for exp in board["expiries"]:
        if exp >= today:
            return exp
    return board["expiries"][-1] if board["expiries"] else None


def _compute(board, expiry, data, spot, spot_source, chain_asof):
    scale = spot * spot * 0.01 * OI_MULTIPLIER / CRORE  # gamma*OI -> crore per 1% move

    rows = []
    for strike_str, sides in data.get("oc", {}).items():
        strike = float(strike_str)
        ce, pe = sides.get("ce") or {}, sides.get("pe") or {}
        call_oi = float(ce.get("oi") or 0)
        put_oi = float(pe.get("oi") or 0)
        if call_oi == 0 and put_oi == 0:
            continue
        call_gamma = float((ce.get("greeks") or {}).get("gamma") or 0)
        put_gamma = float((pe.get("greeks") or {}).get("gamma") or 0)
        call_gex = call_gamma * call_oi * scale
        put_gex = -put_gamma * put_oi * scale
        rows.append({
            "strike": strike,
            "callOi": call_oi,
            "putOi": put_oi,
            "callIv": ce.get("implied_volatility"),
            "putIv": pe.get("implied_volatility"),
            "callGexCr": round(call_gex, 2),
            "putGexCr": round(put_gex, 2),
            "netGexCr": round(call_gex + put_gex, 2),
        })
    rows.sort(key=lambda r: r["strike"])

    cum = 0.0
    for r in rows:
        cum += r["netGexCr"]
        r["cumNetGexCr"] = round(cum, 2)

    # Zero-gamma flip: cumulative net GEX zero-crossing nearest to spot.
    flip = None
    best_dist = None
    for prev, cur in zip(rows, rows[1:]):
        a, b = prev["cumNetGexCr"], cur["cumNetGexCr"]
        if a == 0 or (a < 0) != (b < 0):
            if b == a:
                level = cur["strike"]
            else:
                level = prev["strike"] + (cur["strike"] - prev["strike"]) * (0 - a) / (b - a)
            dist = abs(level - spot)
            if best_dist is None or dist < best_dist:
                best_dist, flip = dist, round(level, 1)

    band = spot * WALL_BAND_PCT / 100
    near = [r for r in rows if abs(r["strike"] - spot) <= band] or rows
    call_wall = max(near, key=lambda r: r["callGexCr"])["strike"] if near else None
    put_wall = min(near, key=lambda r: r["putGexCr"])["strike"] if near else None
    max_pos = max(near, key=lambda r: r["netGexCr"]) if near else None
    max_neg = min(near, key=lambda r: r["netGexCr"]) if near else None

    return {
        "underlying": board["name"],
        "expiry": expiry,
        "spot": spot,
        "spotSource": spot_source,
        "chainAsOf": chain_asof,
        "asOf": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "oiMultiplier": OI_MULTIPLIER,
        "totals": {
            "callGexCr": round(sum(r["callGexCr"] for r in rows), 1),
            "putGexCr": round(sum(r["putGexCr"] for r in rows), 1),
            "netGexCr": round(sum(r["netGexCr"] for r in rows), 1),
        },
        "zeroGammaLevel": flip,
        "callWall": call_wall,
        "putWall": put_wall,
        "maxNetPositive": {"strike": max_pos["strike"], "netGexCr": max_pos["netGexCr"]} if max_pos else None,
        "maxNetNegative": {"strike": max_neg["strike"], "netGexCr": max_neg["netGexCr"]} if max_neg else None,
        "strikeCount": len(rows),
        "strikes": rows,
    }


def _rebuild(board):
    """Recompute one board's snapshot from its cached chain + freshest spot."""
    with _lock:
        chain = board["chain"]
        spot_entry = _spots.get(board["scrip"])
    if not chain:
        return
    if spot_entry and time.time() - spot_entry[1] <= SPOT_FRESH_SECONDS:
        spot, source = spot_entry[0], "quote"
    else:
        spot, source = float(chain["data"]["last_price"]), "chain"
    snap = _compute(board, chain["expiry"], chain["data"], spot, source, chain["asOf"])
    with _lock:
        board["snapshot"] = snap


def _chain_loop():
    """Round-robin the option-chain budget (1 unique request / 3 s, per ACCOUNT) over
    all boards: with N boards each refreshes every N*POLL_SECONDS."""
    global _auth_failed
    idx = 0
    while True:
        board = _boards[idx % len(_boards)]
        idx += 1
        try:
            if not board["expiries"] or board["expiries_on"] != date.today():
                data = _api_post("/optionchain/expirylist",
                                 {"UnderlyingScrip": board["scrip"], "UnderlyingSeg": board["seg"]})
                board["expiries"] = sorted(data)
                board["expiries_on"] = date.today()
                time.sleep(3.2)  # expirylist shares the option-chain rate limit
            expiry = _active_expiry(board)
            if not expiry:
                raise RuntimeError("no expiry available")
            data = _api_post("/optionchain",
                             {"UnderlyingScrip": board["scrip"], "UnderlyingSeg": board["seg"],
                              "Expiry": expiry})
            with _lock:
                board["chain"] = {"expiry": expiry, "data": data,
                                  "asOf": datetime.now(timezone.utc).isoformat(timespec="seconds")}
                board["error"] = None
            _rebuild(board)
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode()[:200]
            except Exception:
                pass
            with _lock:
                board["error"] = f"HTTP {e.code}: {body}"
            if e.code == 401:
                _auth_failed = True
            if e.code == 429:
                time.sleep(10)
        except Exception as e:  # keep serving the last snapshot
            with _lock:
                board["error"] = f"{type(e).__name__}: {e}"
        time.sleep(POLL_SECONDS)


def _spot_loop():
    """Live spot for ALL boards in one batched LTP call per segment (limit: 1 req/s).
    Multiple segments rotate on the same budget."""
    global _spot_error, _auth_failed
    segs = {}
    for b in _boards:
        segs.setdefault(b["seg"], set()).add(b["scrip"])
    seg_list = sorted(segs)
    idx = 0
    while True:
        seg = seg_list[idx % len(seg_list)]
        idx += 1
        try:
            data = _api_post("/marketfeed/ltp", {seg: sorted(segs[seg])})
            now = time.time()
            changed = set()
            with _lock:
                for scrip_str, q in (data.get(seg) or {}).items():
                    ltp = float(q["last_price"])
                    scrip = int(scrip_str)
                    if _spots.get(scrip, (None,))[0] != ltp:
                        changed.add(scrip)
                    _spots[scrip] = (ltp, now)
                _spot_error = None
            for b in _boards:
                if b["scrip"] in changed:
                    _rebuild(b)
        except urllib.error.HTTPError as e:
            with _lock:
                _spot_error = f"HTTP {e.code}"
            if e.code == 401:
                _auth_failed = True
            if e.code == 429:
                time.sleep(5)
        except Exception as e:
            with _lock:
                _spot_error = f"{type(e).__name__}: {e}"
        time.sleep(SPOT_POLL_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        raw = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _board_health(self, board):
        with _lock:
            snap, err, sperr = board["snapshot"], board["error"], _spot_error
        stale = None
        if snap:
            stale = round(time.time() - datetime.fromisoformat(snap["asOf"]).timestamp(), 1)
        self._send(200 if snap else 503, json.dumps({
            "underlying": board["name"],
            "status": "ok" if snap and not err else ("degraded" if snap else "down"),
            "staleSeconds": stale,
            "lastError": err,
            "spotError": sperr,
            "tokenHoursLeft": round((_jwt_exp(ACCESS_TOKEN) - time.time()) / 3600, 2),
            "tokenRenewedAt": _renewed_at,
            "renewError": _renew_error,
            "secretWrittenAt": _secret_written_at,
            "secretError": _secret_error,
        }))

    def _board_gex(self, board):
        with _lock:
            snap, err, sperr = board["snapshot"], board["error"], _spot_error
        if not snap:
            self._send(503, json.dumps({"error": err or "no data yet"}))
        else:
            out = dict(snap)
            out["lastError"] = err
            out["spotError"] = sperr
            self._send(200, json.dumps(out))

    def do_GET(self):
        path = self.path.split("?")[0]
        board = _boards[0]
        sub = path
        for b in _boards:
            prefix = "/" + b["key"]
            if path == prefix:  # canonical trailing slash so relative fetches resolve
                self.send_response(301)
                self.send_header("Location", prefix + "/")
                self.end_headers()
                return
            if path.startswith(prefix + "/"):
                board = b
                sub = path[len(prefix):]
                break
        if sub in ("/", "/index", ""):
            self._send(200, PAGE.replace("__NAME__", board["name"]), "text/html")
        elif sub == "/gex":
            self._board_gex(board)
        elif sub == "/health":
            self._board_health(board)
        else:
            self._send(404, json.dumps({"error": "not found"}))


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>__NAME__ Pressure</title>
<style>
:root{color-scheme:dark}
body{background:#0d1117;color:#c9d1d9;font:13px/1.45 -apple-system,'Segoe UI',sans-serif;margin:0;padding:16px}
h1{font-size:16px;margin:0 0 2px;color:#e6edf3}
#meta{color:#8b949e;margin-bottom:12px}
#cards{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:14px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:8px 14px;min-width:110px}
.card .k{color:#8b949e;font-size:11px;text-transform:uppercase;letter-spacing:.05em}
.card .v{font-size:17px;font-weight:600;margin-top:2px}
.pos{color:#3fb950}.neg{color:#f85149}.neu{color:#e6edf3}
table{border-collapse:collapse;width:100%;max-width:980px}
th,td{padding:2px 8px;text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
th{color:#8b949e;font-weight:500;border-bottom:1px solid #30363d;position:sticky;top:0;background:#0d1117}
td.strike{font-weight:600;color:#e6edf3}
tr.spotrow td{border-top:2px solid #d29922}
tr.flip td{background:#1f2a1f}
.barcell{width:340px;text-align:left;padding:2px 0}
.bar{display:inline-block;height:11px;border-radius:2px;vertical-align:middle}
.barwrap{position:relative;width:340px;height:13px}
.barwrap .zero{position:absolute;left:50%;top:0;bottom:0;width:1px;background:#30363d}
.bpos{position:absolute;left:50%;background:#3fb950;height:11px;top:1px;border-radius:0 2px 2px 0}
.bneg{position:absolute;right:50%;background:#f85149;height:11px;top:1px;border-radius:2px 0 0 2px}
#err{color:#f85149;margin:8px 0}
</style></head><body>
<h1 id="title">__NAME__ Pressure</h1>
<div id="meta">loading…</div>
<div id="err"></div>
<div id="cards"></div>
<table><thead><tr>
<th>Strike</th><th>Put OI</th><th>Call OI</th><th>Put Pressure ₹Cr</th><th>Call Pressure ₹Cr</th>
<th>Net ₹Cr</th><th class="barcell">net pressure</th><th>Cum ₹Cr</th>
</tr></thead><tbody id="rows"></tbody></table>
<script>
const fmt=n=>n==null?"–":n.toLocaleString("en-IN",{maximumFractionDigits:1});
const cls=n=>n>0?"pos":(n<0?"neg":"neu");
async function tick(){
 try{
  const r=await fetch("gex");const d=await r.json();
  if(!r.ok){document.getElementById("err").textContent=d.error||"error";return}
  document.getElementById("err").textContent=d.lastError?("last poll error: "+d.lastError+" (showing last good data)"):"";
  document.getElementById("title").textContent=d.underlying+" Pressure — "+d.expiry;
  document.getElementById("meta").textContent="spot "+fmt(d.spot)+
    (d.spotSource==="quote"?" (live 1s quote)":" (from chain)")+
    " · greeks/OI as of "+(d.chainAsOf||d.asOf)+
    " · ₹crore per 1% move · calls +, puts − · OI×"+d.oiMultiplier;
  const cards=[["Net Pressure",d.totals.netGexCr,cls(d.totals.netGexCr)],
   ["Call Pressure",d.totals.callGexCr,"pos"],["Put Pressure",d.totals.putGexCr,"neg"],
   ["Zero-γ flip",d.zeroGammaLevel,"neu"],["Call wall",d.callWall,"pos"],["Put wall",d.putWall,"neg"]];
  document.getElementById("cards").innerHTML=cards.map(c=>
   '<div class="card"><div class="k">'+c[0]+'</div><div class="v '+c[2]+'">'+fmt(c[1])+'</div></div>').join("");
  const band=d.spot*0.04;
  const rows=d.strikes.filter(s=>Math.abs(s.strike-d.spot)<=band);
  const maxAbs=Math.max(1,...rows.map(s=>Math.abs(s.netGexCr)));
  let spotDone=false;
  document.getElementById("rows").innerHTML=rows.slice().reverse().map(s=>{
   const w=Math.abs(s.netGexCr)/maxAbs*168;
   const bar='<div class="barwrap"><div class="zero"></div><div class="'+(s.netGexCr>=0?"bpos":"bneg")+'" style="width:'+w.toFixed(1)+'px"></div></div>';
   let cl="";
   if(!spotDone&&s.strike<=d.spot){cl="spotrow";spotDone=true}
   if(d.zeroGammaLevel&&Math.abs(s.strike-d.zeroGammaLevel)<25)cl+=" flip";
   return '<tr class="'+cl+'"><td class="strike">'+fmt(s.strike)+'</td><td>'+fmt(s.putOi)+'</td><td>'+fmt(s.callOi)+
    '</td><td class="neg">'+fmt(s.putGexCr)+'</td><td class="pos">'+fmt(s.callGexCr)+
    '</td><td class="'+cls(s.netGexCr)+'">'+fmt(s.netGexCr)+'</td><td class="barcell">'+bar+
    '</td><td class="'+cls(s.cumNetGexCr)+'">'+fmt(s.cumNetGexCr)+'</td></tr>';
  }).join("");
 }catch(e){document.getElementById("err").textContent=String(e)}
}
tick();setInterval(tick,1000);
</script></body></html>"""


def main():
    global _boards
    if not ACCESS_TOKEN or not CLIENT_ID:
        print("DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID must be set", file=sys.stderr)
        sys.exit(1)
    _boards = _parse_boards()
    threading.Thread(target=_chain_loop, daemon=True).start()
    threading.Thread(target=_spot_loop, daemon=True).start()
    threading.Thread(target=_renew_loop, daemon=True).start()
    server = ThreadingHTTPServer((BIND, PORT), Handler)
    print(f"pressure boards on http://{BIND}:{PORT} -> "
          + ", ".join(f"/{b['key']}/ ({b['name']} scrip={b['scrip']} {b['seg']})" for b in _boards),
          flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
