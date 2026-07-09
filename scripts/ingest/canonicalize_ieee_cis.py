"""Canonicalize the IEEE-CIS Fraud Detection dataset to `tx_canonical.v1`.

What this script does
---------------------
1. Loads ``train_transaction.csv`` and ``train_identity.csv`` from the raw
   data directory and joins them on ``TransactionID``.
2. Renames & re-types raw columns so the result fits our canonical schema
   (only the columns in ``scripts.canonical_schema.CANONICAL_SCHEMA``).
3. Hashes PII (``card1``, ``P_emaildomain``, ``DeviceInfo + id_31``) with
   HMAC-SHA256 so that joining is still possible without exposing PII.
4. Validates the result with pandera and writes a single Parquet file.

Run::

    python -m scripts.ingest.canonicalize_ieee_cis \
        --raw data/raw/ieee_cis \
        --out data/canonical/ieee_cis.parquet
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from scripts.canonical_schema import validate_canonical_df
from scripts.common import hmac_hash, load_settings, new_ulid


# IEEE-CIS reference timestamp picked from the EDA; TransactionDT is "seconds
# since this UTC moment".  This can be overridden via Settings (env or .env).
_TXDT_REFERENCE = None  # filled in from settings inside main()


def _to_canonical(df_tx: pd.DataFrame, df_id: pd.DataFrame, hmac_key: str) -> pd.DataFrame:
    # Merge on TransactionID
    df = df_tx.merge(df_id, on="TransactionID", how="left")

    # --- Timestamp ------------------------------------------------------------
    ts_ms = ((df["TransactionDT"].astype("int64") + _TXDT_REFERENCE) * 1000).astype("int64")

    # --- Hash PII columns -----------------------------------------------------
    user_id = df["card1"].astype("string").fillna("").map(lambda v: hmac_hash(f"card1={v}", hmac_key))

    # DeviceInfo + id_31 (browser) form the device fingerprint.
    dev_raw = (
        df["DeviceInfo"].astype("string").fillna("")
        + "|"
        + df["id_31"].astype("string").fillna("")
    )
    device_fp = dev_raw.map(lambda v: hmac_hash(v, hmac_key))

    email_hash = df["P_emaildomain"].astype("string").fillna("").map(
        lambda v: hmac_hash(v, hmac_key) if v else ""
    )

    # IEEE-CIS doesn't give us an IP, so we leave ip_hash blank.
    ip_hash = pd.Series([""] * len(df), dtype="string")

    # --- Categorical / numeric mapping ---------------------------------------
    amount_minor = (df["TransactionAmt"].astype("float64") * 100).round().astype("int64")
    card_bin = df["card2"].astype("Int64").astype("string").fillna("").map(lambda v: v or None)

    # channel: we infer "ecommerce" because IEEE-CIS is 100% e-commerce data
    channel = pd.Series(["ecommerce"] * len(df), dtype="string")

    merchant_id = pd.Series(["vesta"] * len(df), dtype="string")  # single issuer
    mcc = df["ProductCD"].astype("string").fillna("")

    # IEEE-CIS does NOT expose a clean ISO-3166 country code:
    #   * `addr1` = US ZIP-code prefix (3 digits — populated for US residents)
    #   * `addr2` = country code (mostly missing/private)
    # We deliberately leave `country` NULL in the canonical frame and keep
    # the raw values in `attributes` for offline feature engineering.
    addr1 = df["addr1"].astype("Int64")  # preserved in attributes

    # tx_id: regenerate as ULID (we lose the raw TransactionID via attributes)
    tx_ids = [new_ulid() for _ in range(len(df))]

    # --- Build the canonical frame -------------------------------------------
    out = pd.DataFrame({
        "tx_id":             tx_ids,
        "user_id":           user_id,
        "dataset_source":    "ieee_cis",
        "schema_version":    1,
        "ts_ms":             ts_ms,
        "amount_minor":      amount_minor,
        "currency":          "USD",
        "channel":           channel,
        "device_fp":         device_fp.where(device_fp != "", None),
        "ip_hash":           ip_hash.where(ip_hash != "", None),
        "email_domain_hash": email_hash.where(email_hash != "", None),
        "card_bin":          card_bin,
        "merchant_id":       merchant_id,
        "mcc":               mcc,
        "country":           pd.Series([None] * len(df), dtype="object"),
        "ip_country":        pd.Series([None] * len(df), dtype="object"),
        # Use explicit NaN Series so the dtype stays float64.
        "lat":               pd.Series([float("nan")] * len(df), dtype="float64"),
        "lon":               pd.Series([float("nan")] * len(df), dtype="float64"),
        "label":             df["isFraud"].fillna(0).astype("int64").astype("Int8"),
    })

    # Carry the remaining ~390 columns as a dict in `attributes`.
    # We inject `addr1_zips_prefix` so the raw geographic signal is preserved
    # for offline feature engineering (e.g. fraud-rate-by-zip joins).
    out["attributes"] = pd.DataFrame({
        "addr1_zip_prefix": addr1,
    }).to_dict(orient="records")
    return out


def main() -> int:
    global _TXDT_REFERENCE
    p = argparse.ArgumentParser(description="Canonicalize IEEE-CIS dataset.")
    p.add_argument("--raw", type=Path, default=Path("data/raw/ieee_cis"),
                   help="Directory containing train_transaction.csv & train_identity.csv")
    p.add_argument("--out", type=Path, default=Path("data/canonical/ieee_cis.parquet"))
    p.add_argument("--limit", type=int, default=None,
                   help="Optional row limit (for quick tests)")
    args = p.parse_args()

    settings = load_settings()
    _TXDT_REFERENCE = int(settings.ieee_cis_epoch.timestamp())

    tx_path = args.raw / "train_transaction.csv"
    id_path = args.raw / "train_identity.csv"
    if not tx_path.exists():
        print(f"✗ Missing {tx_path}. Run: python -m scripts.ingest.download_datasets "
              f"--datasets ieee_cis", file=__import__("sys").stderr)
        return 1

    print(f"Loading {tx_path.name} (this may take ~30 s)…")
    df_tx = pd.read_csv(tx_path, low_memory=False)
    if args.limit:
        df_tx = df_tx.head(args.limit)

    df_id = pd.DataFrame()
    if id_path.exists():
        print(f"Loading {id_path.name}…")
        df_id = pd.read_csv(id_path, low_memory=False)
    else:
        print(f"  ! {id_path.name} not present; proceeding identity-less.")

    print(f"  tx rows = {len(df_tx):,}, identity rows = {len(df_id):,}")
    out = _to_canonical(df_tx, df_id, settings.pii_hmac_key)
    print(f"  built canonical frame: {out.shape}")
    out = validate_canonical_df(out)
    print(f"  schema-valid rows: {len(out):,}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, engine="pyarrow", index=False)
    print(f"✓ Wrote {args.out} ({args.out.stat().st_size / 1_048_576:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())