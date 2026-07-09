"""Real-time feature computation pipeline (Quix Streams + Redis).

Consumes transactions from ``fds.tx.raw.v1``, computes sliding-window
features using Redis Sorted Sets, and writes the results to Redis Hashes
that the scoring API (Phase 4) will read.

Architecture::

    Redpanda (fds.tx.raw.v1)
        │
        ▼
    Quix StreamingDataFrame  (.apply → compute_and_store_features)
        │
        ▼
    Redis (write-through)
        ├── sw:tx:10m:{user_id}    — Sorted Set (10-min sliding window state)
        ├── sw:txdata:1h:{user_id} — Sorted Set (1-hour sliding window state)
        └── feat:user:{user_id}    — Hash (API-readable computed features)

Features computed
-----------------
    tx_count_10m           Number of transactions in last 10 minutes
    amt_sum_1h             Sum of amounts (cents) in last 1 hour
    max_amt_1h             Max single transaction (cents) in last 1 hour
    distinct_mcc_1h        Distinct MCC codes in last 1 hour
    seconds_since_last_tx  Seconds since user's previous transaction

Usage::

    python -m scripts.feature.stream_processor
"""
from __future__ import annotations

import logging
import os
import time

import redis
from quixstreams import Application

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fds.stream_processor")

# ── Configuration ─────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BROKER = os.environ.get("KAFKA_BROKERS", "localhost:19092")

WINDOW_10M_MS = 10 * 60 * 1000      # 10 minutes in ms
WINDOW_1H_MS = 60 * 60 * 1000       # 1 hour in ms

TTL_WINDOW_10M = 20 * 60            # 20 min TTL (2× window)
TTL_WINDOW_1H = 2 * 60 * 60         # 2 hours TTL (2× window)
TTL_FEATURES = 24 * 60 * 60         # 24 hours TTL for computed features

# ── Redis connection ──────────────────────────────────────────────────────
redis_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    """Lazy-init Redis connection."""
    global redis_client
    if redis_client is None:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return redis_client


# ── Sliding-window feature computation ────────────────────────────────────

def compute_and_store_features(row: dict) -> dict:
    """Process one transaction: update sliding windows and compute features.

    Uses Redis Sorted Sets for accurate sliding windows:
    - Members are unique tx data, scores are timestamps.
    - Old entries outside the window are pruned on each event.
    - Features are computed from the remaining window contents.

    The computed features are stored in a Redis Hash for the API to read.
    """
    user_id = row.get("user_id")
    if not user_id:
        return row

    tx_id = row.get("tx_id", "")
    amount_minor = int(row.get("amount_minor", 0))
    mcc = str(row.get("mcc") or "unknown")
    ts_ms = int(row.get("ts_ms") or int(time.time() * 1000))

    # Redis keys
    key_tx_10m = f"sw:tx:10m:{user_id}"
    key_txdata_1h = f"sw:txdata:1h:{user_id}"
    key_features = f"feat:user:{user_id}"
    key_last_ts = f"sw:last_ts:{user_id}"

    cutoff_10m = ts_ms - WINDOW_10M_MS
    cutoff_1h = ts_ms - WINDOW_1H_MS

    r = get_redis()

    try:
        pipe = r.pipeline(transaction=False)

        # ── 1) Add to sliding windows ────────────────────────────────
        # 10-min window: member = tx_id, score = ts_ms
        pipe.zadd(key_tx_10m, {tx_id: ts_ms})
        # 1-hour window: member = "tx_id|amount|mcc", score = ts_ms
        member_1h = f"{tx_id}|{amount_minor}|{mcc}"
        pipe.zadd(key_txdata_1h, {member_1h: ts_ms})

        # ── 2) Prune expired entries ─────────────────────────────────
        pipe.zremrangebyscore(key_tx_10m, "-inf", cutoff_10m)
        pipe.zremrangebyscore(key_txdata_1h, "-inf", cutoff_1h)

        # ── 3) Read current window contents ──────────────────────────
        pipe.zcard(key_tx_10m)                       # → tx_count_10m
        pipe.zrange(key_txdata_1h, 0, -1)            # → all 1h members
        pipe.get(key_last_ts)                        # → previous ts

        # ── 4) Update last-seen timestamp ────────────────────────────
        pipe.set(key_last_ts, str(ts_ms), ex=TTL_FEATURES)

        # ── 5) Set TTLs on window keys ───────────────────────────────
        pipe.expire(key_tx_10m, TTL_WINDOW_10M)
        pipe.expire(key_txdata_1h, TTL_WINDOW_1H)

        results = pipe.execute()

        # ── Parse results (indices match pipeline order) ─────────────
        # 0: zadd tx_10m, 1: zadd txdata_1h,
        # 2: zrem tx_10m, 3: zrem txdata_1h,
        # 4: zcard tx_10m, 5: zrange txdata_1h,
        # 6: get last_ts, 7: set last_ts,
        # 8: expire tx_10m, 9: expire txdata_1h

        tx_count_10m = results[4]
        members_1h = results[5]  # list of "tx_id|amount|mcc" strings
        prev_ts_str = results[6]

        # ── Compute 1-hour aggregate features ────────────────────────
        amt_sum_1h = 0
        max_amt_1h = 0
        mcc_set = set()

        for member in members_1h:
            parts = member.rsplit("|", 2)
            if len(parts) == 3:
                _, amt_str, m = parts
                try:
                    amt = int(amt_str)
                except (ValueError, TypeError):
                    amt = 0
                amt_sum_1h += amt
                if amt > max_amt_1h:
                    max_amt_1h = amt
                mcc_set.add(m)

        distinct_mcc_1h = len(mcc_set)

        # ── Compute seconds_since_last_tx ────────────────────────────
        if prev_ts_str:
            try:
                prev_ts = int(prev_ts_str)
                seconds_since_last = max(0, (ts_ms - prev_ts)) / 1000.0
            except (ValueError, TypeError):
                seconds_since_last = -1
        else:
            seconds_since_last = -1  # first transaction ever

        # ── 6) Write computed features to Hash ───────────────────────
        feature_map = {
            "tx_count_10m": tx_count_10m,
            "amt_sum_1h": amt_sum_1h,
            "max_amt_1h": max_amt_1h,
            "distinct_mcc_1h": distinct_mcc_1h,
            "seconds_since_last_tx": round(seconds_since_last, 2),
            "last_updated_ms": ts_ms,
        }
        pipe2 = r.pipeline(transaction=False)
        pipe2.hset(key_features, mapping=feature_map)
        pipe2.expire(key_features, TTL_FEATURES)
        pipe2.execute()

        # Inject features into the row for downstream consumers
        row["_features"] = feature_map

    except redis.RedisError as exc:
        logger.error("Redis error for user %s: %s", user_id[:12], exc)

    return row


# ── Logging callback ──────────────────────────────────────────────────────

_log_counter = 0


def log_transaction(row: dict) -> dict:
    """Log every Nth processed transaction for observability."""
    global _log_counter
    _log_counter += 1
    if _log_counter % 100 == 0:
        tx_id = row.get("tx_id", "?")
        user_id = row.get("user_id", "?")
        feats = row.get("_features", {})
        logger.info(
            "Processed %d | TX %s | User %s… | tx_10m=%s amt_1h=%s",
            _log_counter, tx_id[:10], user_id[:8],
            feats.get("tx_count_10m", "?"),
            feats.get("amt_sum_1h", "?"),
        )
    return row


# ── Quix Streams application ─────────────────────────────────────────────

def build_app() -> Application:
    """Construct the Quix Streams application (testable factory)."""
    app = Application(
        broker_address=BROKER,
        consumer_group="fds-stream-processor-group",
        auto_offset_reset="earliest",
    )
    input_topic = app.topic("fds.tx.raw.v1", value_deserializer="json")
    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(compute_and_store_features)
    sdf = sdf.apply(log_transaction)
    return app


if __name__ == "__main__":
    logger.info("Starting FDS Stream Processor")
    logger.info("  Broker : %s", BROKER)
    logger.info("  Redis  : %s", REDIS_URL)

    r = get_redis()
    logger.info("  Redis PING: %s", r.ping())

    app = build_app()
    logger.info("Consuming from fds.tx.raw.v1 ...")
    app.run()
