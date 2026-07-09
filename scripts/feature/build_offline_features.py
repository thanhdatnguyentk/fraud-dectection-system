"""Offline feature engineering using DuckDB.

Reads canonical Parquet files and computes per-user aggregate features
for model training. Output is a single Parquet with one row per user.

Features computed
-----------------
    tx_count_30d           Total transaction count
    amt_mean_30d           Mean transaction amount (cents)
    amt_std_30d            Std dev of transaction amount
    amt_max_30d            Max transaction amount
    amt_min_30d            Min transaction amount
    distinct_mcc_30d       Number of distinct MCC codes
    distinct_merchant_30d  Number of distinct merchants
    distinct_country_30d   Number of distinct countries
    fraud_rate_user        Fraction of user's transactions that are fraud
    pct_ecommerce          Fraction of transactions via ecommerce channel
    pct_card_present       Fraction via card_present
    pct_mobile             Fraction via mobile
    avg_seconds_between_tx Average gap between consecutive transactions

Usage::

    python -m scripts.feature.build_offline_features \\
        --input data/canonical/ieee_cis.parquet \\
        --output data/features/offline/ieee_cis_features.parquet
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("fds.features")


def build_features(input_path: str) -> pd.DataFrame:
    """Build per-user aggregate features from a canonical Parquet file.

    Parameters
    ----------
    input_path : str
        Path to canonical Parquet file (must have tx_canonical.v1 schema).

    Returns
    -------
    pd.DataFrame
        One row per user_id with all computed features.
    """
    logger.info("Building offline features from %s", input_path)

    con = duckdb.connect(":memory:")

    # Register the Parquet file as a table
    con.execute(f"CREATE VIEW tx AS SELECT * FROM read_parquet('{input_path}')")

    row_count = con.execute("SELECT COUNT(*) FROM tx").fetchone()[0]
    user_count = con.execute("SELECT COUNT(DISTINCT user_id) FROM tx").fetchone()[0]
    logger.info("Input: %d rows, %d unique users", row_count, user_count)

    # Build aggregate features per user using SQL.
    # Use a CTE to compute per-row window features (LAG), then aggregate.
    query = """
    WITH tx_with_gap AS (
        SELECT
            *,
            (ts_ms - LAG(ts_ms) OVER (PARTITION BY user_id ORDER BY ts_ms)) / 1000.0
                AS gap_seconds
        FROM tx
    )
    SELECT
        user_id,

        -- Transaction count & amounts
        COUNT(*)                                    AS tx_count_30d,
        AVG(amount_minor)                           AS amt_mean_30d,
        STDDEV_POP(amount_minor)                    AS amt_std_30d,
        MAX(amount_minor)                           AS amt_max_30d,
        MIN(amount_minor)                           AS amt_min_30d,

        -- Diversity features
        COUNT(DISTINCT mcc)                         AS distinct_mcc_30d,
        COUNT(DISTINCT merchant_id)                 AS distinct_merchant_30d,
        COUNT(DISTINCT country)                     AS distinct_country_30d,

        -- Fraud rate (label-based, for training only)
        COALESCE(AVG(CASE WHEN label = 1 THEN 1.0 ELSE 0.0 END), 0) AS fraud_rate_user,

        -- Channel distribution
        AVG(CASE WHEN channel = 'ecommerce' THEN 1.0 ELSE 0.0 END)    AS pct_ecommerce,
        AVG(CASE WHEN channel = 'card_present' THEN 1.0 ELSE 0.0 END) AS pct_card_present,
        AVG(CASE WHEN channel = 'mobile' THEN 1.0 ELSE 0.0 END)       AS pct_mobile,

        -- Time-based features (from CTE window)
        COALESCE(AVG(gap_seconds), 0) AS avg_seconds_between_tx,

        -- Latest label (for training target assignment)
        MAX(label) AS has_fraud_label

    FROM tx_with_gap
    GROUP BY user_id
    """

    result = con.execute(query).fetchdf()

    # Fill NaN in std with 0 (users with single transaction)
    result["amt_std_30d"] = result["amt_std_30d"].fillna(0)
    result["avg_seconds_between_tx"] = result["avg_seconds_between_tx"].fillna(0)

    con.close()

    logger.info("Built %d feature rows with %d columns", len(result), len(result.columns))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build offline per-user features from canonical Parquet",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input", type=str, default="data/canonical/ieee_cis.parquet",
        help="Input canonical Parquet file",
    )
    parser.add_argument(
        "--output", type=str, default="data/features/offline/ieee_cis_features.parquet",
        help="Output features Parquet file",
    )
    args = parser.parse_args()

    result = build_features(args.input)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out_path, engine="pyarrow", index=False)
    logger.info("Saved features to %s (%.1f KB)", out_path, out_path.stat().st_size / 1024)

    # Print summary
    print(f"\n{'='*50}")
    print(f"Feature Summary: {len(result)} users")
    print(f"{'='*50}")
    for col in result.columns:
        if col == "user_id":
            continue
        print(f"  {col:30s}  mean={result[col].mean():>10.2f}  std={result[col].std():>10.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
