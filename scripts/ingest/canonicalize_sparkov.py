"""Canonicalize the Sparkov synthetic dataset (Kartik2112 Kaggle mirror).

Notes
-----
* Sparkov CSV columns of interest:
  - ``trans_date_trans_time`` (ISO timestamp)
  - ``cc_num`` (credit-card number — tokenized)
  - ``merchant`` (merchant name — tokenized)
  - ``category`` / ``amt`` / ``lat`` / ``long``
  - ``city`` / ``state`` / ``zip`` / ``city_pop``
  - ``dob`` (date-of-birth — drop, age derive hashed)
  - ``is_fraud`` (label)
* ``cc_num`` is the join key for ``user_id``.  It is HMAC-hashed.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from scripts.canonical_schema import validate_canonical_df
from scripts.common import hmac_hash, load_settings, new_ulid


def _to_canonical(df: pd.DataFrame, hmac_key: str) -> pd.DataFrame:
    # ---- time ----
    # pandas returns datetime64[us, UTC] (microsecond resolution). To get
    # epoch *milliseconds* we divide by 1000 (us→ms), not 1_000_000 (which
    # would yield epoch seconds and lose precision).
    ts_ms = pd.to_datetime(df["trans_date_trans_time"], utc=True).astype("int64") // 1000

    # ---- people ----
    user_id = df["cc_num"].astype("string").fillna("").map(lambda v: hmac_hash(f"cc={v}", hmac_key))
    dev_fp = df["cc_num"].astype("string").fillna("") + "|" + df["first"].astype("string").fillna("")
    device_fp = dev_fp.map(lambda v: hmac_hash(v, hmac_key))

    # ---- merchant / category ----
    merchant = df["merchant"].astype("string").fillna("").map(lambda v: hmac_hash(f"merch={v}", hmac_key))
    mcc = df["category"].astype("string").fillna("")

    # ---- geo ----
    lat = pd.to_numeric(df["lat"], errors="coerce").astype("float64")
    lon = pd.to_numeric(df["long"], errors="coerce").astype("float64")
    country = "US"  # Sparkov is exclusively US

    # ---- money ----
    amount_minor = (df["amt"].astype("float64") * 100).round().astype("int64")

    out = pd.DataFrame({
        "tx_id":             [new_ulid() for _ in range(len(df))],
        "user_id":           user_id,
        "dataset_source":    "sparkov",
        "schema_version":    1,
        "ts_ms":             ts_ms.astype("int64"),
        "amount_minor":      amount_minor,
        "currency":          "USD",
        "channel":           "card_present",
        "device_fp":         device_fp,
        "ip_hash":           pd.Series([None] * len(df), dtype="object"),
        "email_domain_hash": pd.Series([None] * len(df), dtype="object"),
        "card_bin":          df["cc_num"].astype("string").str.slice(0, 6),
        "merchant_id":       merchant,
        "mcc":               mcc,
        "country":          pd.Series([country] * len(df), dtype="object"),
        "ip_country":       pd.Series([country] * len(df), dtype="object"),
        "lat":               lat,
        "lon":               lon,
        "label":             df["is_fraud"].fillna(0).astype("int64").astype("Int8"),
    })

    out["attributes"] = df.drop(columns=["is_fraud"]).to_dict(orient="records")
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Canonicalize Sparkov dataset.")
    p.add_argument(
        "--raw",
        type=Path,
        # Kaggle's kartik2112/fraud-detection ships:
        #   fraudTrain.csv  (training, ~1.2M rows)
        #   fraudTest.csv   (test set, ~555K rows)
        # We default to fraudTrain.csv; switch with --raw for test or other splits.
        default=Path("data/raw/sparkov/fraudTrain.csv"),
    )
    p.add_argument("--out", type=Path, default=Path("data/canonical/sparkov.parquet"))
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    if not args.raw.exists():
        print(f"✗ Missing {args.raw}. Run: python -m scripts.ingest.download_datasets "
              f"--datasets sparkov", file=__import__("sys").stderr)
        return 1

    settings = load_settings()
    df = pd.read_csv(args.raw)
    if args.limit:
        df = df.head(args.limit)
    print(f"Loaded {len(df):,} rows from {args.raw.name}")

    out = _to_canonical(df, settings.pii_hmac_key)
    out = validate_canonical_df(out)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, engine="pyarrow", index=False)
    print(f"✓ Wrote {args.out} ({args.out.stat().st_size / 1_048_576:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())