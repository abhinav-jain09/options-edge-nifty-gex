#!/usr/bin/env python3
"""Offline test for expiry-day rollover: on the expiry day the board keeps that expiry
through the session and rolls to the NEXT expiry at ROLL_AFTER_IST (default 17:30 IST,
two hours after the 15:30 close). Run: python3 test_expiry_roll.py"""

import os
import sys
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault("DHAN_ACCESS_TOKEN", "x")
os.environ.setdefault("DHAN_CLIENT_ID", "x")
import gex_service as g

BOARD = {"expiries": ["2026-09-01", "2026-09-08", "2026-09-15"]}


def at_ist(day, hhmm):
    h, m = map(int, hhmm.split(":"))
    ist = datetime.fromisoformat(day).replace(hour=h, minute=m, tzinfo=timezone.utc)
    return ist - timedelta(hours=5, minutes=30)  # UTC instant whose IST is day hh:mm


def expiry_at(day, hhmm):
    with mock.patch.object(g, "datetime", mock.Mock(wraps=datetime)) as md:
        md.now.return_value = at_ist(day, hhmm)
        return g._active_expiry(BOARD)


checks = [
    ("expiry-day morning keeps today", expiry_at("2026-09-01", "09:20"), "2026-09-01"),
    ("expiry-day 15:29 keeps today", expiry_at("2026-09-01", "15:29"), "2026-09-01"),
    ("expiry-day 17:29 still today", expiry_at("2026-09-01", "17:29"), "2026-09-01"),
    ("expiry-day 17:30 ROLLS", expiry_at("2026-09-01", "17:30"), "2026-09-08"),
    ("expiry-day 21:00 rolled", expiry_at("2026-09-01", "21:00"), "2026-09-08"),
    ("next morning still next expiry", expiry_at("2026-09-02", "09:20"), "2026-09-08"),
    ("non-expiry day untouched by cutoff", expiry_at("2026-09-03", "18:00"), "2026-09-08"),
    ("last expiry never rolls past the end", None, None),
]
ok = True
for name, got, want in checks[:-1]:
    passed = got == want
    ok &= passed
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}: {got}")

with mock.patch.object(g, "datetime", mock.Mock(wraps=datetime)) as md:
    md.now.return_value = at_ist("2026-09-15", "18:00")
    got = g._active_expiry({"expiries": ["2026-09-15"]})
passed = got == "2026-09-15"
ok &= passed
print(f"  [{'PASS' if passed else 'FAIL'}] last expiry never rolls past the end: {got}")

print("ALL PASS" if ok else "FAILED")
sys.exit(0 if ok else 1)
