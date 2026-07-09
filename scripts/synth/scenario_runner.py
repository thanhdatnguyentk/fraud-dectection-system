"""Scenario-based transaction generator for fraud testing.

Generates specific attack patterns and pushes them directly to Redpanda
to test the stream processor's detection capabilities.

Scenarios
---------
    velocity_attack    — 30 tx in 60s from the same user
    impossible_travel  — NY → Tokyo in 5 minutes
    device_spray       — 1 device fingerprint across 50 users
    fat_finger         — amount 10× user's average
    burst_spike        — 3× normal traffic in 60s

Usage::

    python -m scripts.synth.scenario_runner --scenario velocity_attack
    python -m scripts.synth.scenario_runner --scenario all --tps 200
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

from quixstreams import Application

from scripts.common import hmac_hash, load_settings, new_ulid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fds.scenario_runner")

BROKER = os.environ.get("KAFKA_BROKERS", "localhost:19092")
TOPIC_NAME = "fds.tx.raw.v1"


def _make_tx(
    user_id: str,
    amount_cents: int,
    mcc: str = "5411",
    channel: str = "ecommerce",
    country: str = "US",
    lat: float = 40.7128,
    lon: float = -74.0060,
    device_fp: str | None = None,
) -> dict:
    """Build a canonical-compatible transaction dict."""
    settings = load_settings()
    if device_fp is None:
        device_fp = hmac_hash(f"dev-{user_id}", settings.pii_hmac_key)
    return {
        "tx_id": new_ulid(),
        "user_id": user_id,
        "dataset_source": "synthetic",
        "schema_version": 1,
        "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "amount_minor": amount_cents,
        "currency": "USD",
        "channel": channel,
        "device_fp": device_fp,
        "ip_hash": hmac_hash(f"ip-{user_id[-6:]}", settings.pii_hmac_key),
        "email_domain_hash": hmac_hash("test@gmail.com", settings.pii_hmac_key),
        "card_bin": "448588",
        "merchant_id": "m_test",
        "mcc": mcc,
        "country": country,
        "ip_country": country,
        "lat": lat,
        "lon": lon,
        "label": None,
    }


def _send_batch(records: list[dict], tps: int = 100):
    """Send a list of transaction records to Redpanda."""
    app = Application(broker_address=BROKER)
    topic = app.topic(TOPIC_NAME, value_serializer="json")

    with app.get_producer() as producer:
        for i, record in enumerate(records):
            user_id = str(record.get("user_id", ""))
            msg = topic.serialize(key=user_id, value=record)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)

            if tps > 0 and (i + 1) % tps == 0:
                time.sleep(1.0)

        producer.flush()
    logger.info("Sent %d records to %s", len(records), TOPIC_NAME)


# ── Scenario implementations ─────────────────────────────────────────────

def scenario_velocity_attack(n_tx: int = 30, window_sec: int = 60, tps: int = 100):
    """Simulate 30 transactions from the same card within 60 seconds."""
    settings = load_settings()
    user_id = hmac_hash("velocity-attacker-001", settings.pii_hmac_key)
    logger.info("=== VELOCITY ATTACK: %d tx in %ds, user=%s… ===", n_tx, window_sec, user_id[:12])

    records = []
    for i in range(n_tx):
        tx = _make_tx(user_id, amount_cents=5000 + i * 100, mcc="5411", channel="card_present")
        records.append(tx)

    _send_batch(records, tps)
    logger.info("Velocity attack complete. Check Redis: HGETALL feat:user:%s", user_id[:12] + "...")


def scenario_impossible_travel(tps: int = 100):
    """Simulate a transaction in New York, then Tokyo 5 minutes later."""
    settings = load_settings()
    user_id = hmac_hash("traveler-impossible-001", settings.pii_hmac_key)
    logger.info("=== IMPOSSIBLE TRAVEL: NY → Tokyo in 5 min, user=%s… ===", user_id[:12])

    records = [
        _make_tx(user_id, 15000, country="US", lat=40.7128, lon=-74.0060),
    ]
    _send_batch(records, tps)
    logger.info("TX from New York sent. Waiting 3 seconds...")
    time.sleep(3)

    records = [
        _make_tx(user_id, 25000, country="JP", lat=35.6762, lon=139.6503),
    ]
    _send_batch(records, tps)
    logger.info("TX from Tokyo sent. distinct_country should increase.")


def scenario_device_spray(n_users: int = 50, tps: int = 100):
    """Simulate one device fingerprint shared across 50 different users."""
    settings = load_settings()
    shared_device = hmac_hash("suspicious-device-001", settings.pii_hmac_key)
    logger.info("=== DEVICE SPRAY: 1 device, %d users, device=%s… ===", n_users, shared_device[:12])

    records = []
    for i in range(n_users):
        user_id = hmac_hash(f"spray-victim-{i:04d}", settings.pii_hmac_key)
        tx = _make_tx(user_id, 8000 + i * 50, device_fp=shared_device)
        records.append(tx)

    _send_batch(records, tps)
    logger.info("Device spray complete.")


def scenario_fat_finger(tps: int = 100):
    """Simulate normal transactions then a 10× amount spike."""
    settings = load_settings()
    user_id = hmac_hash("fat-finger-user-001", settings.pii_hmac_key)
    logger.info("=== FAT FINGER: normal then 10×, user=%s… ===", user_id[:12])

    # 5 normal transactions (~$50 each)
    records = [_make_tx(user_id, 5000) for _ in range(5)]
    _send_batch(records, tps)
    logger.info("5 normal transactions sent. Waiting 2 seconds...")
    time.sleep(2)

    # 1 fat-finger transaction ($500)
    records = [_make_tx(user_id, 50000)]
    _send_batch(records, tps)
    logger.info("Fat finger TX sent. max_amt_1h should spike.")


def scenario_burst_spike(n_tx: int = 300, tps: int = 300):
    """Simulate a sudden burst of traffic (3× normal)."""
    settings = load_settings()
    logger.info("=== BURST SPIKE: %d tx at %d TPS ===", n_tx, tps)

    records = []
    for i in range(n_tx):
        user_id = hmac_hash(f"burst-user-{i % 20:04d}", settings.pii_hmac_key)
        tx = _make_tx(user_id, 3000 + (i * 7) % 5000)
        records.append(tx)

    _send_batch(records, tps)
    logger.info("Burst spike complete.")


# ── Scenario registry ────────────────────────────────────────────────────

SCENARIOS = {
    "velocity_attack": scenario_velocity_attack,
    "impossible_travel": scenario_impossible_travel,
    "device_spray": scenario_device_spray,
    "fat_finger": scenario_fat_finger,
    "burst_spike": scenario_burst_spike,
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run fraud-testing scenarios against Redpanda",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scenario", type=str, default="velocity_attack",
        choices=list(SCENARIOS.keys()) + ["all"],
        help="Scenario to run",
    )
    parser.add_argument("--tps", type=int, default=100, help="Target TPS")
    args = parser.parse_args()

    if args.scenario == "all":
        for name, func in SCENARIOS.items():
            logger.info("Running scenario: %s", name)
            func(tps=args.tps)
            time.sleep(2)
    else:
        SCENARIOS[args.scenario](tps=args.tps)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
