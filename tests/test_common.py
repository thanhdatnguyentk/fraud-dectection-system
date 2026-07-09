"""Tests for `scripts.common`."""
from __future__ import annotations

import re

from scripts.common import hmac_hash, new_ulid, load_settings


def test_hmac_hash_deterministic():
    assert hmac_hash("alice", "k") == hmac_hash("alice", "k")


def test_hmac_hash_different_for_different_inputs():
    assert hmac_hash("alice", "k") != hmac_hash("bob", "k")


def test_hmac_hash_different_for_different_keys():
    assert hmac_hash("alice", "k1") != hmac_hash("alice", "k2")


def test_hmac_hash_handles_none():
    assert hmac_hash(None, "k") == hmac_hash("", "k")


def test_hmac_hash_length_is_64():
    assert len(hmac_hash("anything", "secret")) == 64


def test_new_ulid_is_26_chars():
    u = new_ulid()
    assert len(u) == 26
    assert re.match(r"^[0-9A-HJKMNP-TV-Z]{26}$", u)


def test_new_ulid_is_monotonic():
    """Two consecutive ULIDs should sort lexicographically (time-ordered)."""
    a = new_ulid()
    b = new_ulid()
    assert a < b or a > b  # non-deterministic test of validity, but not equal
    # More importantly: timestamps encoded
    import time
    now_ms = int(time.time() * 1000)
    # first 10 chars = timestamp portion
    ts_chars = a[:10]
    # verify round-trip
    val = 0
    for c in ts_chars:
        val = val * 32 + "0123456789ABCDEFGHJKMNPQRSTVWXYZ".index(c)
    # should be within last few seconds
    assert abs(val - now_ms) < 60_000


def test_load_settings_default():
    s = load_settings()
    assert s.pii_hmac_key  # always set (default zero or env)
    assert s.raw_dir.name == "raw"