"""Canonicalize the ULB Credit Card Fraud (PCA-anonymised) dataset.

Notes
-----
* ULB exposes ``Time`` (seconds since first row) and ``Amount`` plus the
  PCA-anonymised ``V1..V28`` features.  We *cannot* reconstruct user_id,
  device_fp, geo, etc. — those columns are therefore NULL in the canonical
  frame.
* `Time` is converted to a synthetic ``ts_ms`` anchored at ``now() - 1 day``.
  This keeps the schema valid while the absolute timestamp has no meaning.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.canonical_schema import validate_canonical_df
from scripts.common import hmac_hash, load_settings, new_ulid


def _to_canonical(df: pd.DataFrame, hmac_key: str) -> pd.DataFrame:
    # Each row becomes its own pseudo-user: ULB does not provide an identity.
    n = len(df)
    user_ids = [hmac_hash(f"ulb-row-{i}", hmac_key) for i in range(n)]

    # PCA-anonymised rows have no card info → generate a stable but distinct BIN
    # for hashing key.
    card_bins = [f"{(i % 9 + 1) * 100000 + (i * 37 % 99999)}" for i in range(n)]

    # Anchor at "now - 2 days" and space events by `Time` seconds.
    base = datetime.now(timezone.utc) - timedelta(days=2)
    ts_ms = ((df["Time"].astype("int64")) + int(base.timestamp())) * 1000
    ts_ms = ts_ms.astype("int64")

    out = pd.DataFrame({
        "tx_id":             [new_ulid() for _ in range(n)],
        "user_id":           user_ids,
        "dataset_source":    "ulb",
        "schema_version":    1,
        "ts_ms":             ts_ms,
        "amount_minor":      (df["Amount"].astype("float64") * 100).round().astype("int64"),
        "currency":          "EUR",
        "channel":           "card_present",
        "device_fp":         pd.Series([None] * n, dtype="object"),
        "ip_hash":           pd.Series([None] * n, dtype="object"),
        "email_domain_hash": pd.Series([None] * n, dtype="object"),
        "card_bin":          card_bins,
        "merchant_id":       pd.Series([None] * n, dtype="object"),
        "mcc":               pd.Series([None] * n, dtype="object"),
        "country":           pd.Series([None] * n, dtype="object"),
        "ip_country":        pd.Series([None] * n, dtype="object"),
        # Use float64-dtyped NA columns; otherwise pandas picks `object` and
        # pandera rejects the dtype.  Each field is constructed as a Series of
        # NaN to lock the dtype down.
        "lat":               pd.Series([float("nan")] * n, dtype="float64"),
        "lon":               pd.Series([float("nan")] * n, dtype="float64"),
        "label":             df["Class"].fillna(0).astype("int64").astype("Int8"),
    })

    # Drop the original PCA columns in `attributes` (preserved for offline use)
    out["attributes"] = df[["Time", "Amount"] + [f"V{i}" for i in range(1, 29)]].to_dict(orient="records")
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Canonicalize ULB credit-card dataset.")
    p.add_argument("--raw", type=Path, default=Path("data/raw/ulb/creditcard.csv"))
    p.add_argument("--out", type=Path, default=Path("data/canonical/ulb.parquet"))
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    if not args.raw.exists():
        print(f"✗ Missing {args.raw}. Run: python -m scripts.ingest.download_datasets "
              f"--datasets ulb", file=__import__("sys").stderr)
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
    print(f"✓ Wrote {args.out} ({args.out.stat().st_size / 1_048_576:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())