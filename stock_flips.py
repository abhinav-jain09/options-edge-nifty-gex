"""The near-flip scan over the NIFTY stocks' option boards — pressure flip levels, in ₹.

A direct port of the US closing-board flip scan, applied to live Dhan
chains instead of frozen closing boards. Three lists, same three meanings:

``nearFlip``       spot sits on (or almost on) the zero crossing of the cumulative
                   per-strike net pressure — the dampen-the-move / chase-the-move boundary.
``positiveDominant`` the owner's screen: near the flip AND a LADDER of positive strikes
                   (several comparable rungs, no single wall), ranked by posShare.
``knifeEdge``      |net| is a sliver of gross — the LONG/SHORT label is one flow from
                   inverting.

Differences from the US scan, all deliberate:
* Boards are LIVE (each stock's nearest monthly chain, refreshed round-robin on the shared
  Dhan budget), not a frozen closing generation — every row carries its own asOf.
* Pressure is ₹ crore per 1% move (the vendor's per-contract curvature greek * OI *
  spot^2 * 0.01, OI already lot-included), so the
  gross floors are ₹ figures: MIN_GROSS_CR / KNIFE_MIN_GROSS_CR. Initial calibration only —
  env-tunable, and they should be re-cut once a few sessions of Indian boards exist, the
  way the US floors were cut on 2026-08-28 boards.
* No expiry window: NSE stock options trade ONE liquid monthly; the chain IS the window.

The universe lives in ``stock_universe.txt`` (SYMBOL:securityId, '#' comments) — the
NIFTY 50 constituents at build time, kept as a data file because membership drifts and
the owner will edit it. Empty/missing is an ERROR, exactly like the US page's allow-list.
"""

import os
import re
from pathlib import Path

MIN_GROSS_CR = float(os.environ.get("DHAN_FLIPS_MIN_GROSS_CR", "25"))
KNIFE_MIN_GROSS_CR = float(os.environ.get("DHAN_FLIPS_KNIFE_MIN_GROSS_CR", "100"))
LIST_LIMIT = int(os.environ.get("DHAN_FLIPS_LIST_LIMIT", "20"))
NEAR_BAND_PCT = float(os.environ.get("DHAN_FLIPS_NEAR_BAND_PCT", "2.0"))
LADDER_RUNG_SHARE = 0.25
LADDER_MIN_RUNGS = 4
LADDER_TOP_MAX = 1.0 / 3.0

SCHEMA_VERSION = 1

UNIVERSE_FILE = Path(__file__).parent / "stock_universe.txt"


def load_universe(path=UNIVERSE_FILE):
    """[(symbol, security_id), ...] — empty or missing is an error, never a quiet blank page."""
    entries = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.fullmatch(r"([A-Z0-9&\-]+):(\d+)", line)
        if not m:
            raise ValueError(f"unparseable universe line: {line!r}")
        entries.append((m.group(1), int(m.group(2))))
    if not entries:
        raise ValueError(f"the stock universe at {path} is empty")
    return entries


def board_rows(chain_data, spot, crore=1e7):
    """Per-strike net pressure (₹Cr per 1% move) from one Dhan chain — [(strike, net)], ascending.

    Same convention as the index boards: calls positive, puts negative, OI in underlying
    units (lot already included).
    """
    scale = spot * spot * 0.01 / crore
    rows = []
    for strike_str, sides in (chain_data.get("oc") or {}).items():
        try:
            strike = float(strike_str)
        except (TypeError, ValueError):
            continue
        ce, pe = sides.get("ce") or {}, sides.get("pe") or {}
        call_oi = float(ce.get("oi") or 0)
        put_oi = float(pe.get("oi") or 0)
        if call_oi == 0 and put_oi == 0:
            continue
        # "gamma" below is the VENDOR's field name inside greeks{} — the one string this
        # feature cannot rename.
        call_curve = float((ce.get("greeks") or {}).get("gamma") or 0)
        put_curve = float((pe.get("greeks") or {}).get("gamma") or 0)
        net = (call_curve * call_oi - put_curve * put_oi) * scale
        rows.append((strike, net))
    rows.sort(key=lambda r: r[0])
    return rows


def nearest_crossing(rows, spot):
    """The strike where the cumulative net sum CHANGES SIGN, nearest the spot.

    Sign of the running sum is what carries meaning; a shelf at exactly zero counts once,
    where it ends — sign() of the previous NON-ZERO sum decides (ported verbatim from the
    US scan, including that subtlety).
    """
    crossings = []
    running = 0.0
    prev_sign = 0
    for strike, net in rows:
        running += net
        sign = 1 if running > 0 else (-1 if running < 0 else 0)
        if sign != 0 and prev_sign != 0 and sign != prev_sign:
            crossings.append(strike)
        if sign != 0:
            prev_sign = sign
    if not crossings:
        return None
    return min(crossings, key=lambda k: abs(k - spot))


def flip_scan(boards):
    """The whole scan, pure: {symbol: {"spot", "rows", "asOf", "expiry"}} in, ranked lists out."""
    near, dominant, knife = [], [], []
    seen = 0
    for symbol, b in boards.items():
        spot = b.get("spot")
        rows = b.get("rows")
        if not rows or not isinstance(spot, (int, float)) or spot <= 0:
            continue
        seen += 1
        pos = sum(net for _, net in rows if net > 0)
        neg = sum(-net for _, net in rows if net < 0)
        gross = pos + neg
        if gross <= 0 or gross < MIN_GROSS_CR:
            continue
        total = pos - neg
        regime = "LONG" if total > 0 else "SHORT"
        positive_nets = [net for _, net in rows if net > 0]
        max_rung = max(positive_nets) if positive_nets else 0.0
        rungs = sum(1 for net in positive_nets if net >= LADDER_RUNG_SHARE * max_rung) \
            if max_rung > 0 else 0
        top_share = (max_rung / pos) if pos > 0 else 0.0
        flip = nearest_crossing(rows, spot)
        if flip is not None:
            row = {
                "symbol": symbol,
                "spot": spot,
                "flipLevel": flip,
                # signed: positive = the spot trades ABOVE its flip level
                "spotVsFlipPct": round(100.0 * (spot - flip) / spot, 3),
                "regime": regime,
                "netPressureCr": round(total, 2),
                "grossPressureCr": round(gross, 2),
                "posShare": round(pos / gross, 4),
                "positiveRungs": rungs,
                "topStrikeShare": round(top_share, 4),
                "expiry": b.get("expiry"),
                "asOf": b.get("asOf"),
            }
            near.append(row)
            if (abs(row["spotVsFlipPct"]) <= NEAR_BAND_PCT
                    and rungs >= LADDER_MIN_RUNGS
                    and top_share <= LADDER_TOP_MAX):
                dominant.append(row)
        if gross >= KNIFE_MIN_GROSS_CR:
            knife.append({
                "symbol": symbol,
                "spot": spot,
                "regime": regime,
                "netPressureCr": round(total, 2),
                "grossPressureCr": round(gross, 2),
                "netPctOfGross": round(100.0 * total / gross, 2),
                "asOf": b.get("asOf"),
            })
    near.sort(key=lambda r: abs(r["spotVsFlipPct"]))
    dominant.sort(key=lambda r: (-r["posShare"], abs(r["spotVsFlipPct"])))
    knife.sort(key=lambda r: abs(r["netPctOfGross"]))
    return {
        "schemaVersion": SCHEMA_VERSION,
        "boardsSeen": seen,
        "nearFlip": near[:LIST_LIMIT],
        "positiveDominant": dominant[:LIST_LIMIT],
        "knifeEdge": knife[:LIST_LIMIT],
    }
