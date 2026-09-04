#!/usr/bin/env python3
"""Offline tests for the NIFTY stock flip-level scan — the cases the US suite guards, in ₹.
Run: python3 test_stock_flips.py"""

import sys

import stock_flips as sf

ok = True


def check(name, cond):
    global ok
    ok &= bool(cond)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")


# --- nearest_crossing ------------------------------------------------------------------
# Simple sign change between strikes: cumulative -5, -2, +3 → crossing at the third strike.
check("crossing where the running sum flips sign",
      sf.nearest_crossing([(100, -5), (110, 3), (120, 5)], 118) == 120)
# A shelf at EXACTLY zero is not a crossing until the sum leaves zero on the other side.
check("zero shelf counts once, where it ends",
      sf.nearest_crossing([(100, -5), (110, 5), (120, 4)], 115) == 120)
# Same-side re-touch: -5 → 0 → -3 never crosses.
check("touching zero and returning is no crossing",
      sf.nearest_crossing([(100, -5), (110, 5), (120, -3)], 110) is None)
# Two crossings: the one nearest spot wins.
check("nearest of two crossings wins",
      sf.nearest_crossing([(100, -5), (110, 6), (120, -7), (130, 8)], 128) == 130)
check("no rows, no crossing", sf.nearest_crossing([], 100) is None)

# --- board_rows ------------------------------------------------------------------------
chain = {"oc": {
    "100.000000": {"ce": {"oi": 1000, "greeks": {"gamma": 0.02}},
                   "pe": {"oi": 500, "greeks": {"gamma": 0.01}}},
    "110.000000": {"ce": {"oi": 0, "greeks": {"gamma": 0}}, "pe": {"oi": 0}},
    "junk": {"ce": {"oi": 10, "greeks": {"gamma": 0.1}}, "pe": {}},
}}
rows = sf.board_rows(chain, spot=100.0)
check("board_rows keeps only priced strikes with OI",
      len(rows) == 1 and rows[0][0] == 100.0)
# net = (0.02*1000 - 0.01*500) * 100^2 * 0.01 / 1e7 = 0.00015 — which sits exactly ON the
# 4dp rounding edge, so compare against the unrounded value with a tolerance wider than
# the rounding step rather than re-rounding (float repr makes the two paths round apart).
check("board_rows net matches the index-board formula",
      abs(rows[0][1] - 15 * 100 / 1e7) <= 6e-5)
detail = sf.board_detail(chain, spot=100.0)
check("board_detail carries the index-board wire shape",
      set(detail[0]) == {"strike", "callOi", "putOi", "callGexCr", "putGexCr",
                         "netGexCr", "cumNetGexCr"})


# --- flip_scan admissions --------------------------------------------------------------
def board(rows, spot, sym="X"):
    return {sym: {"spot": spot, "rows": rows, "asOf": "t", "expiry": "2026-09-29"}}


def big(rows):
    # scale rows so gross clears MIN_GROSS_CR comfortably
    f = 10 * sf.MIN_GROSS_CR / max(1e-9, sum(abs(n) for _, n in rows))
    return [(k, n * f) for k, n in rows]


# cumulative: -20, -12, -4, +3 ... -> the sign change lands at 100, one strike above spot
ladder = big([(90, -20), (95, 8), (98, 8), (100, 7), (105, 7), (110, 6)])
scan = sf.flip_scan(board(ladder, 99.0))
check("near-flip row produced with flip level + signed distance",
      scan["nearFlip"] and scan["nearFlip"][0]["flipLevel"] == 100
      and scan["nearFlip"][0]["spotVsFlipPct"] < 0)
check("ladder of comparable rungs admits positiveDominant",
      len(scan["positiveDominant"]) == 1)

wall = big([(90, -40), (100, 41), (105, 2), (110, 2)])
scan = sf.flip_scan(board(wall, 99.0))
check("one wall with trim is NOT a ladder (topStrikeShare gate)",
      scan["nearFlip"] and not scan["positiveDominant"])

tiny = [(90, -0.4), (100, 0.5)]
scan = sf.flip_scan(board(tiny, 99.0))
check("gross below the ₹ floor is discarded as noise",
      scan["boardsSeen"] == 1 and not scan["nearFlip"])

sliver = big([(90, -50), (100, 51)])
scan = sf.flip_scan(board(sliver, 95.0))
check("knife edge ranks by |net|/gross and needs the stricter floor",
      bool(scan["knifeEdge"]) == (sum(abs(n) for _, n in sliver) >= sf.KNIFE_MIN_GROSS_CR))

# --- universe file ---------------------------------------------------------------------
u = sf.load_universe()
# The count is the OWNER's to change (growth names only since 2026-09-04) — assert the file
# parses and is non-trivial, not a membership number that his edits would break.
check("universe parses and is non-trivial", 10 <= len(u) <= 210)
check("symbols with & and - parse (M&M, BAJAJ-AUTO)",
      any(s == "M&M" for s, _ in u) and any(s == "BAJAJ-AUTO" for s, _ in u))

print("ALL PASS" if ok else "FAILED")
sys.exit(0 if ok else 1)
