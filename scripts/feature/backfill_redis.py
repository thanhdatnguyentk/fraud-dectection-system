"""Backfill offline features into Redis for scoring.

Reads the offline features Parquet file and writes them to Redis Hashes
under the key pattern `offline:user:{user_id}`.

Usage::

    python -m scripts.feature.backfill_redis \\
        --input data/features/offline/ieee_cis_features.parquet
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

import pandas as pd
import redis.asyncio as redis
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("fds.backfill")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

async def backfill(input_path: str, batch_size: int = 5000):
    logger.info("Connecting to Redis at %s", REDIS_URL)
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    try:
        await r.ping()
    except Exception as e:
        logger.error("Redis connection failed: %s", e)
        return

    logger.info("Reading %s", input_path)
    df = pd.read_parquet(input_path)
    
    # We don't need user_id as a feature, nor has_fraud_label
    cols_to_store = [c for c in df.columns if c not in ("user_id", "has_fraud_label")]
    
    logger.info("Found %d users, %d features", len(df), len(cols_to_store))
    
    # Convert all columns to string for Redis storage
    for c in cols_to_store:
        if df[c].dtype.kind in 'bifc':
            # Format floats to save space, ints keep as is
            df[c] = df[c].astype(str)

    records = df.to_dict(orient="records")
    
    logger.info("Starting pipeline backfill in batches of %d", batch_size)
    
    total = len(records)
    for i in tqdm(range(0, total, batch_size)):
        batch = records[i:i+batch_size]
        async with r.pipeline(transaction=False) as pipe:
            for row in batch:
                user_id = row["user_id"]
                mapping = {k: row[k] for k in cols_to_store}
                key = f"offline:user:{user_id}"
                pipe.hset(key, mapping=mapping)
            await pipe.execute()

    logger.info("Backfill complete!")
    await r.aclose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill offline features to Redis")
    parser.add_argument("--input", type=str, default="data/features/offline/ieee_cis_features.parquet")
    args = parser.parse_args()

    if not Path(args.input).exists():
        logger.error("Input file %s does not exist", args.input)
        return 1

    asyncio.run(backfill(args.input))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
