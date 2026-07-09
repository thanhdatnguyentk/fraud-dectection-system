"""Canonicalize the PaySim mobile-money fraud dataset.

Notes
-----
* Used primarily for GNN training (``nameOrig -> nameDest`` chains).
* PaySim's ``step`` is an hour index; we anchor at a fixed UTC reference.
* Output keeps ``orig_hash`` and ``dest_hash`` in `attributes` for graph
  construction (we also expose them as canonical user_ids for accounts,
  but transactions still have a single ``user_id`` set to the originator).
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from scripts.canonical_schema import validate_canonical_df
from scripts.common import hmac_hash, load_settings, new_ulid


# PaySim `step` = hours since the reference instant below (2018-01-01 UTC).
PAYSIM_REFERENCE = datetime(2018, 1, 1, tzinfo=timezone.utc)


def _to_canonical(df: pd.DataFrame, hmac_key: str) -> pd.DataFrame:
    ts_ms = (
        df["step"].astype("int64") * 3600
        + int(PAYSIM_REFERENCE.timestamp())
    ) * 1000
    ts_ms = ts_ms.astype("int64")

    user_id = df["nameOrig"].astype("string").fillna("").map(
        lambda v: hmac_hash(f"orig={v}", hmac_key)
    )
    dest_hash = df["nameDest"].astype("string").fillna("").map(
        lambda v: hmac_hash(f"dest={v}", hmac_key)
    )

    # PaySim amount may be 0 for failed transactions; clamp to ≥ 0
    amount_minor = (df["amount"].astype("float64") * 100).round().clip(lower=0).astype("int64")

    # Map PaySim transaction type → our channel enum
    type_to_channel = {
        "PAYMENT":      "transfer",
        "TRANSFER":     "transfer",
        "CASH_OUT":     "atm",
        "CASH_IN":      "atm",
        "DEBIT":        "card_present",
        "PAY_SALARY":   "transfer",
    }
    channel = df["type"].astype("string").map(type_to_channel).fillna("other").astype("string")

    # Balance differences can be negative; normalize
    old_bal = pd.to_numeric(df["oldbalanceOrg"], errors="coerce")
    new_bal = pd.to_numeric(df["newbalanceOrig"], errors="coerce")
    deltas = (new_bal - old_bal).astype("float64")

    out = pd.DataFrame({
        "tx_id":             [new_ulid() for _ in range(len(df))],
        "user_id":           user_id,
        "dataset_source":    "paysim",
        "schema_version":    1,
        "ts_ms":             ts_ms,
        "amount_minor":      amount_minor,
        "currency":          "USD",
        "channel":           channel,
        "device_fp":         pd.Series([None] * len(df), dtype="object"),
        "ip_hash":           pd.Series([None] * len(df), dtype="object"),
        "email_domain_hash": pd.Series([None] * len(df), dtype="object"),
        "card_bin":          pd.Series([None] * len(df), dtype="object"),
        "merchant_id":       dest_hash,           # dest plays the role of payee
        "mcc":               df["type"].astype("string"),
        "country":           pd.Series([None] * len(df), dtype="object"),
        "ip_country":        pd.Series([None] * len(df), dtype="object"),
        # Use explicit NaN Series so the dtype stays float64.
        "lat":               pd.Series([float("nan")] * len(df), dtype="float64"),
        "lon":               pd.Series([float("nan")] * len(df), dtype="float64"),
        "label":             df["isFraud"].fillna(0).astype("int64").astype("Int8"),
    })

    out["attributes"] = pd.DataFrame({
        "orig_hash":          user_id,
        "dest_hash":          dest_hash,
        "oldbalanceOrg":      old_bal,
        "newbalanceOrig":     new_bal,
        "oldbalanceDest":     pd.to_numeric(df["oldbalanceDest"], errors="coerce"),
        "newbalanceDest":     pd.to_numeric(df["newbalanceDest"], errors="coerce"),
        "balance_delta":      deltas,
        "isFlaggedFraud":     df["isFlaggedFraud"].fillna(0).astype("int64"),
    }).to_dict(orient="records")

    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Canonicalize PaySim dataset.")
    # The Kaggle mirror ships the CSV with a numeric prefix in the filename
    # (e.g. ``PS_20174392719_1491204439457_log.csv``).  If the user passes a
    # directory, we auto-pick the first ``.csv`` we find under it.
    p.add_argument(
        "--raw",
        type=Path,
        default=Path("data/raw/paysim"),
        help="Path to a CSV file or directory containing PaySim CSV.",
    )
    p.add_argument("--out", type=Path, default=Path("data/canonical/paysim.parquet"))
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    if not args.raw.exists():
        print(f"✗ Missing {args.raw}. Run: python -m scripts.ingest.download_datasets "
              f"--datasets paysim", file=__import__("sys").stderr)
        return 1

    # If --raw is a directory, auto-resolve the first CSV inside.
    csv_path = args.raw
    if csv_path.is_dir():
        candidates = sorted(csv_path.glob("*.csv"))
        if not candidates:
            print(f"✗ No CSV found inside {csv_path.resolve()}", file=__import__("sys").stderr)
            return 1
        csv_path = candidates[0]
        print(f"Resolved directory to {csv_path.name}")

    settings = load_settings()
    df = pd.read_csv(csv_path)
    if args.limit:
        df = df.head(args.limit)
    print(f"Loaded {len(df):,} rows from {csv_path.name}")

    out = _to_canonical(df, settings.pii_hmac_key)
    out = validate_canonical_df(out)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, engine="pyarrow", index=False)
    print(f"✓ Wrote {args.out} ({args.out.stat().st_size / 1_048_576:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())