#!/usr/bin/env python3
"""Connectivity test for the Dhan API before running the GEX service.

Checks, in order:
  1. token present + not expired (decodes the JWT exp locally)
  2. expiry list endpoint reachable and returning dates
  3. option chain for the nearest expiry returns spot, strikes, and greeks

Exit 0 = all good, 1 = something failed. Run via: ./test.sh
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime

API = "https://api.dhan.co/v2"
TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
SCRIP = int(os.environ.get("DHAN_GEX_UNDERLYING_SCRIP", "13"))
SEG = os.environ.get("DHAN_GEX_UNDERLYING_SEG", "IDX_I")

ok = True


def check(name, passed, detail=""):
    global ok
    ok = ok and passed
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def post(path, body):
    req = urllib.request.Request(
        API + path, data=json.dumps(body).encode(),
        headers={"access-token": TOKEN, "client-id": CLIENT_ID,
                 "Content-Type": "application/json"}, method="POST")
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode()), time.time() - t0


print("1. Credentials")
check("DHAN_ACCESS_TOKEN set", bool(TOKEN))
check("DHAN_CLIENT_ID set", bool(CLIENT_ID), CLIENT_ID)
if TOKEN:
    try:
        payload = TOKEN.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        exp = datetime.fromtimestamp(claims["exp"])
        hours_left = (claims["exp"] - time.time()) / 3600
        check("token not expired", hours_left > 0, f"expires {exp} ({hours_left:.1f}h left)")
        check("token clientId matches", str(claims.get("dhanClientId")) == CLIENT_ID)
    except Exception as e:
        check("token decodes as JWT", False, str(e))
if not ok:
    sys.exit(1)

print("2. Expiry list  (POST /optionchain/expirylist)")
expiry = None
try:
    resp, dt = post("/optionchain/expirylist", {"UnderlyingScrip": SCRIP, "UnderlyingSeg": SEG})
    dates = resp.get("data") or []
    future = [d for d in sorted(dates) if d >= date.today().isoformat()]
    expiry = future[0] if future else None
    check("status=success", resp.get("status") == "success", f"{dt*1000:.0f} ms")
    check("has upcoming expiry", expiry is not None, f"{len(dates)} dates, nearest {expiry}")
except urllib.error.HTTPError as e:
    check("request", False, f"HTTP {e.code}: {e.read().decode()[:150]}")
except Exception as e:
    check("request", False, f"{type(e).__name__}: {e}")
if not ok:
    sys.exit(1)

time.sleep(3.2)  # Dhan rate limit: 1 request / 3 s

print(f"3. Option chain  (POST /optionchain, expiry {expiry})")
try:
    resp, dt = post("/optionchain",
                    {"UnderlyingScrip": SCRIP, "UnderlyingSeg": SEG, "Expiry": expiry})
    data = resp.get("data") or {}
    oc = data.get("oc") or {}
    spot = data.get("last_price")
    check("status=success", resp.get("status") == "success", f"{dt*1000:.0f} ms")
    check("spot present", bool(spot), f"spot={spot}")
    check("strikes present", len(oc) > 0, f"{len(oc)} strikes")
    with_oi = [s for s, v in oc.items()
               if (v.get("ce") or {}).get("oi") or (v.get("pe") or {}).get("oi")]
    check("strikes with OI", len(with_oi) > 0, f"{len(with_oi)} strikes carry OI")
    gammas = [(v.get("ce") or {}).get("greeks", {}).get("gamma") for v in oc.values()]
    check("greeks/gamma present", any(g for g in gammas if g))
except urllib.error.HTTPError as e:
    check("request", False, f"HTTP {e.code}: {e.read().decode()[:150]}")
except Exception as e:
    check("request", False, f"{type(e).__name__}: {e}")

print("\nALL CHECKS PASSED ✓" if ok else "\nFAILED ✗")
sys.exit(0 if ok else 1)
