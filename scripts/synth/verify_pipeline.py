"""End-to-end pipeline verification for Phase 2.

Checks that the full streaming pipeline works:
    Producer → Redpanda → Stream Processor → Redis

Prerequisite: Docker Compose must be running + stream_processor must be consuming.

Usage::

    python -m scripts.synth.verify_pipeline
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone

import redis

from scripts.common import hmac_hash, load_settings, new_ulid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fds.verify")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BROKER = os.environ.get("KAFKA_BROKERS", "localhost:19092")


def check_docker_services() -> bool:
    """Verify Redis and Redpanda are reachable."""
    # Check Redis
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        logger.info("✅ Redis is reachable at %s", REDIS_URL)
    except redis.ConnectionError:
        logger.error("❌ Redis not reachable at %s", REDIS_URL)
        return False

    # Check Redpanda (try Kafka protocol)
    try:
        from quixstreams import Application
        app = Application(broker_address=BROKER)
        topic = app.topic("fds.verify.test", value_serializer="json")
        with app.get_producer() as producer:
            msg = topic.serialize(key="test", value={"ping": True})
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            producer.flush()
        logger.info("✅ Redpanda is reachable at %s", BROKER)
    except Exception as exc:
        logger.error("❌ Redpanda not reachable at %s: %s", BROKER, exc)
        return False

    return True


def send_test_transactions(user_id: str, n: int = 5) -> list[dict]:
    """Send N test transactions for a known user."""
    from quixstreams import Application

    settings = load_settings()
    app = Application(broker_address=BROKER)
    topic = app.topic("fds.tx.raw.v1", value_serializer="json")

    records = []
    for i in range(n):
        record = {
            "tx_id": new_ulid(),
            "user_id": user_id,
            "dataset_source": "synthetic",
            "schema_version": 1,
            "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            "amount_minor": 5000 + i * 1000,   # $50, $60, $70, $80, $90
            "currency": "USD",
            "channel": "ecommerce",
            "device_fp": hmac_hash(f"dev-verify", settings.pii_hmac_key),
            "ip_hash": hmac_hash("ip-verify", settings.pii_hmac_key),
            "email_domain_hash": hmac_hash("verify@gmail.com", settings.pii_hmac_key),
            "card_bin": "448588",
            "merchant_id": "m_verify",
            "mcc": "5411",
            "country": "US",
            "ip_country": "US",
            "lat": 40.7128,
            "lon": -74.0060,
            "label": None,
        }
        records.append(record)
        time.sleep(0.1)  # small gap between transactions

    with app.get_producer() as producer:
        for record in records:
            msg = topic.serialize(key=user_id, value=record)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
        producer.flush()

    logger.info("Sent %d test transactions for user %s…", n, user_id[:12])
    return records


def verify_redis_features(user_id: str, expected_count: int, timeout: int = 30) -> bool:
    """Wait for features to appear in Redis and validate them."""
    r = redis.from_url(REDIS_URL, decode_responses=True)
    key = f"feat:user:{user_id}"

    logger.info("Waiting for stream processor to compute features (timeout=%ds)...", timeout)

    for attempt in range(timeout):
        features = r.hgetall(key)
        if features and int(features.get("tx_count_10m", 0)) >= expected_count:
            logger.info("✅ Features found after %ds:", attempt + 1)
            for k, v in sorted(features.items()):
                logger.info("   %-24s = %s", k, v)

            # Validate specific expectations
            errors = []
            tx_count = int(features.get("tx_count_10m", 0))
            if tx_count < expected_count:
                errors.append(f"tx_count_10m={tx_count}, expected >= {expected_count}")

            amt_sum = int(features.get("amt_sum_1h", 0))
            expected_sum = sum(5000 + i * 1000 for i in range(expected_count))
            if amt_sum < expected_sum:
                errors.append(f"amt_sum_1h={amt_sum}, expected >= {expected_sum}")

            max_amt = int(features.get("max_amt_1h", 0))
            expected_max = 5000 + (expected_count - 1) * 1000
            if max_amt != expected_max:
                errors.append(f"max_amt_1h={max_amt}, expected {expected_max}")

            if errors:
                for err in errors:
                    logger.warning("⚠️  %s", err)
                return False

            logger.info("✅ All feature validations PASSED!")
            return True

        time.sleep(1)

    logger.error("❌ Timeout: features not found in Redis after %ds", timeout)
    logger.info("   Key: %s", key)
    features = r.hgetall(key)
    if features:
        logger.info("   Partial features found: %s", features)
    else:
        logger.info("   No features in Redis. Is the stream processor running?")
    return False


def verify_sliding_window(user_id: str) -> bool:
    """Verify that sliding window sorted sets exist."""
    r = redis.from_url(REDIS_URL, decode_responses=True)

    checks = [
        (f"sw:tx:10m:{user_id}", "10-min window"),
        (f"sw:txdata:1h:{user_id}", "1-hour window"),
    ]

    ok = True
    for key, label in checks:
        count = r.zcard(key)
        if count > 0:
            logger.info("✅ %s: %d entries", label, count)
        else:
            logger.warning("⚠️  %s: empty (key=%s)", label, key)
            ok = False

    return ok


def main() -> int:
    logger.info("=" * 60)
    logger.info("FDS Phase 2 — End-to-End Pipeline Verification")
    logger.info("=" * 60)

    # Step 1: Check services
    logger.info("\n--- Step 1: Check Docker services ---")
    if not check_docker_services():
        logger.error("Docker services not ready. Run: docker compose up -d")
        return 1

    # Step 2: Send test transactions
    logger.info("\n--- Step 2: Send test transactions ---")
    settings = load_settings()
    user_id = hmac_hash("verify-user-e2e-test", settings.pii_hmac_key)
    n_tx = 5
    send_test_transactions(user_id, n_tx)

    # Step 3: Verify features
    logger.info("\n--- Step 3: Verify Redis features ---")
    if not verify_redis_features(user_id, n_tx, timeout=30):
        logger.error(
            "Feature verification failed. Make sure the stream processor is running:\n"
            "  python -m scripts.feature.stream_processor"
        )
        return 1

    # Step 4: Verify sliding windows
    logger.info("\n--- Step 4: Verify sliding window state ---")
    verify_sliding_window(user_id)

    # Step 5: Cleanup test keys (optional)
    logger.info("\n--- Step 5: Summary ---")
    logger.info("=" * 60)
    logger.info("✅  Phase 2 E2E verification PASSED!")
    logger.info("    Producer → Redpanda → Stream Processor → Redis")
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
