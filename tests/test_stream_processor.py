"""Unit tests for the stream processor's feature computation.

Tests the sliding window logic by mocking Redis with fakeredis.
These tests do NOT require Docker to be running.
"""
from __future__ import annotations

import time

import pytest

try:
    import fakeredis
    HAS_FAKEREDIS = True
except ImportError:
    HAS_FAKEREDIS = False

from scripts.common import hmac_hash, load_settings, new_ulid


def _make_tx(user_id: str, amount_minor: int = 5000, mcc: str = "5411", ts_ms: int | None = None) -> dict:
    """Build a minimal canonical transaction dict."""
    return {
        "tx_id": new_ulid(),
        "user_id": user_id,
        "amount_minor": amount_minor,
        "mcc": mcc,
        "ts_ms": ts_ms or int(time.time() * 1000),
    }


@pytest.fixture
def fake_redis():
    """Create a fakeredis instance and patch the stream processor."""
    if not HAS_FAKEREDIS:
        pytest.skip("fakeredis not installed")

    server = fakeredis.FakeServer()
    r = fakeredis.FakeRedis(server=server, decode_responses=True)
    return r


@pytest.fixture
def compute_fn(fake_redis):
    """Return compute_and_store_features with a patched Redis client."""
    import scripts.feature.stream_processor as sp
    original = sp.redis_client
    sp.redis_client = fake_redis
    yield sp.compute_and_store_features
    sp.redis_client = original


class TestSlidingWindowFeatures:
    """Test that sliding-window features are computed correctly."""

    def test_single_transaction_sets_count_to_1(self, compute_fn, fake_redis):
        settings = load_settings()
        user_id = hmac_hash("test-user-001", settings.pii_hmac_key)
        row = _make_tx(user_id, amount_minor=10000)

        result = compute_fn(row)

        features = fake_redis.hgetall(f"feat:user:{user_id}")
        assert int(features["tx_count_10m"]) == 1
        assert int(features["amt_sum_1h"]) == 10000
        assert int(features["max_amt_1h"]) == 10000
        assert int(features["distinct_mcc_1h"]) == 1

    def test_multiple_transactions_accumulate(self, compute_fn, fake_redis):
        settings = load_settings()
        user_id = hmac_hash("test-user-002", settings.pii_hmac_key)

        for i in range(5):
            row = _make_tx(user_id, amount_minor=1000 * (i + 1), mcc=f"541{i}")
            compute_fn(row)

        features = fake_redis.hgetall(f"feat:user:{user_id}")
        assert int(features["tx_count_10m"]) == 5
        # 1000 + 2000 + 3000 + 4000 + 5000 = 15000
        assert int(features["amt_sum_1h"]) == 15000
        assert int(features["max_amt_1h"]) == 5000
        assert int(features["distinct_mcc_1h"]) == 5

    def test_seconds_since_last_tx(self, compute_fn, fake_redis):
        settings = load_settings()
        user_id = hmac_hash("test-user-003", settings.pii_hmac_key)

        now_ms = int(time.time() * 1000)
        row1 = _make_tx(user_id, ts_ms=now_ms)
        compute_fn(row1)

        features1 = fake_redis.hgetall(f"feat:user:{user_id}")
        # First tx ever → -1
        assert float(features1["seconds_since_last_tx"]) == -1

        # Second tx 10 seconds later
        row2 = _make_tx(user_id, ts_ms=now_ms + 10_000)
        compute_fn(row2)

        features2 = fake_redis.hgetall(f"feat:user:{user_id}")
        assert float(features2["seconds_since_last_tx"]) == 10.0

    def test_10min_window_expires_old_entries(self, compute_fn, fake_redis):
        settings = load_settings()
        user_id = hmac_hash("test-user-004", settings.pii_hmac_key)

        now_ms = int(time.time() * 1000)
        window_10m_ms = 10 * 60 * 1000

        # TX 15 minutes ago (outside window)
        old_row = _make_tx(user_id, amount_minor=9999, ts_ms=now_ms - window_10m_ms - 5 * 60 * 1000)
        compute_fn(old_row)

        # TX now (inside window)
        new_row = _make_tx(user_id, amount_minor=1000, ts_ms=now_ms)
        compute_fn(new_row)

        features = fake_redis.hgetall(f"feat:user:{user_id}")
        # Only the new TX should be in the 10-min window
        assert int(features["tx_count_10m"]) == 1
        # But both should be in the 1-hour window
        assert int(features["amt_sum_1h"]) == 9999 + 1000

    def test_1h_window_expires_old_entries(self, compute_fn, fake_redis):
        settings = load_settings()
        user_id = hmac_hash("test-user-005", settings.pii_hmac_key)

        now_ms = int(time.time() * 1000)
        window_1h_ms = 60 * 60 * 1000

        # TX 2 hours ago (outside 1h window)
        old_row = _make_tx(user_id, amount_minor=9999, ts_ms=now_ms - window_1h_ms - 60 * 60 * 1000)
        compute_fn(old_row)

        # TX now
        new_row = _make_tx(user_id, amount_minor=1000, ts_ms=now_ms)
        compute_fn(new_row)

        features = fake_redis.hgetall(f"feat:user:{user_id}")
        # Only new TX should remain in 1h window
        assert int(features["amt_sum_1h"]) == 1000
        assert int(features["max_amt_1h"]) == 1000

    def test_skip_row_without_user_id(self, compute_fn, fake_redis):
        row = {"tx_id": new_ulid(), "amount_minor": 5000}
        result = compute_fn(row)
        assert result == row  # returned unchanged
        # No Redis keys should be created
        assert len(fake_redis.keys("feat:*")) == 0

    def test_features_injected_into_row(self, compute_fn, fake_redis):
        settings = load_settings()
        user_id = hmac_hash("test-user-006", settings.pii_hmac_key)
        row = _make_tx(user_id)
        result = compute_fn(row)
        assert "_features" in result
        assert "tx_count_10m" in result["_features"]
        assert result["_features"]["tx_count_10m"] == 1
