"""High-throughput Parquet → Redpanda transaction producer.

Reads canonical Parquet files sorted by ``ts_ms`` and streams them into
Redpanda topic ``fds.tx.raw.v1`` at a configurable TPS with batch
production for sustained throughput up to 500+ TPS on a single machine.

Usage::

    # Smoke test (100 TPS, sample data)
    python -m scripts.synth.kafka_producer --file data/canonical/sample.parquet --tps 100

    # Load test (500 TPS, Sparkov dataset, cap at 50k messages)
    python -m scripts.synth.kafka_producer --file data/canonical/sparkov.parquet --tps 500 --max 50000

    # Full replay with speedup (IEEE-CIS, 200 TPS)
    python -m scripts.synth.kafka_producer --file data/canonical/ieee_cis.parquet --tps 200
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from pathlib import Path

import pandas as pd
from quixstreams import Application

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fds.producer")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Shutdown signal received, finishing current batch...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Core producer
# ---------------------------------------------------------------------------

def _clean_record(rec: dict) -> dict:
    """Replace NaN/NaT with None for JSON serialization."""
    out = {}
    for k, v in rec.items():
        if isinstance(v, float) and v != v:  # fast NaN check
            out[k] = None
        elif v is pd.NaT:
            out[k] = None
        else:
            out[k] = v
    return out


def produce_transactions(
    file_path: str,
    tps: int = 100,
    max_msgs: int = 0,
    batch_size: int = 50,
    topic_name: str = "fds.tx.raw.v1",
) -> int:
    """Stream Parquet rows into Redpanda at *tps* transactions per second.

    Returns the total number of messages produced.
    """
    path = Path(file_path)
    if not path.exists():
        logger.error("File not found: %s", file_path)
        logger.error("Generate sample data first: python -m scripts.synth.generate_sample")
        sys.exit(1)

    broker = os.environ.get("KAFKA_BROKERS", "localhost:19092")
    logger.info("Connecting to Redpanda at %s", broker)

    app = Application(broker_address=broker)
    topic = app.topic(topic_name, value_serializer="json")

    # ── Load & prepare ────────────────────────────────────────────────────
    logger.info("Loading %s ...", file_path)
    df = pd.read_parquet(file_path)

    # Sort by timestamp for time-ordered replay
    if "ts_ms" in df.columns:
        df = df.sort_values("ts_ms").reset_index(drop=True)

    total_rows = len(df)
    if max_msgs > 0:
        total_rows = min(total_rows, max_msgs)

    # Pre-convert to list of dicts — 10-50× faster than iterrows()
    logger.info("Converting %d rows to dicts...", total_rows)
    records = [_clean_record(r) for r in df.head(total_rows).to_dict("records")]

    logger.info(
        "Ready: %d records | target %d TPS | batch size %d",
        len(records), tps, batch_size,
    )

    # ── Produce with rate limiting ────────────────────────────────────────
    batch_interval = batch_size / tps  # seconds we should spend per batch
    produced = 0
    wall_start = time.monotonic()
    batch_start = time.monotonic()
    batch_count = 0

    with app.get_producer() as producer:
        for record in records:
            if _shutdown:
                logger.info("Shutdown requested, stopping.")
                break

            user_id = str(record.get("user_id", ""))
            msg = topic.serialize(key=user_id, value=record)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)

            produced += 1
            batch_count += 1

            # Flush + rate-limit per batch
            if batch_count >= batch_size:
                producer.flush()
                elapsed = time.monotonic() - batch_start
                sleep_time = batch_interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
                batch_start = time.monotonic()
                batch_count = 0

            # Progress report every 1000 messages
            if produced % 1000 == 0:
                elapsed_total = time.monotonic() - wall_start
                actual_tps = produced / elapsed_total if elapsed_total > 0 else 0
                pct = produced / len(records) * 100
                logger.info(
                    "Progress: %s/%s (%.1f%%) | Actual: %.0f TPS",
                    f"{produced:,}", f"{len(records):,}", pct, actual_tps,
                )

        # Final flush
        producer.flush()

    elapsed_total = time.monotonic() - wall_start
    actual_tps = produced / elapsed_total if elapsed_total > 0 else 0
    logger.info(
        "Done. Produced %s messages in %.1fs (avg %.0f TPS)",
        f"{produced:,}", elapsed_total, actual_tps,
    )
    return produced


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stream canonical Parquet transactions into Redpanda",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--file", type=str, default="data/canonical/sample.parquet",
        help="Path to canonical Parquet file",
    )
    parser.add_argument("--tps", type=int, default=100, help="Target transactions per second")
    parser.add_argument("--max", type=int, default=0, help="Max messages (0 = all rows)")
    parser.add_argument("--batch", type=int, default=50, help="Batch size for production")
    parser.add_argument(
        "--topic", type=str, default="fds.tx.raw.v1",
        help="Redpanda topic name",
    )
    args = parser.parse_args()

    produce_transactions(args.file, args.tps, args.max, args.batch, args.topic)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
