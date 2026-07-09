"""Generate a small synthetic dataset in canonical form.

Use case
--------
While waiting for Kaggle credentials (or running unit tests offline), we still
need a self-contained sample of `tx_canonical.v1` rows to:

* drive the rest of the data pipeline (parquet → Iceberg → Kafka),
* feed integration tests,
* demo the schema to teammates.

The generator is **deterministic** (seeded RNG) so two runs produce the same
output, making tests reproducible.

Run::

    python -m scripts.synth.generate_sample --rows 1000 --out data/canonical/sample.parquet
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from scripts.canonical_schema import validate_canonical_df
from scripts.common import hmac_hash, load_settings, new_ulid


# --- distributions we want to be reproducible ------------------------------

FIRST_NAMES = [
    "alice", "bob", "carol", "dan", "eve", "frank", "grace", "heidi", "ivan",
    "judy", "mallory", "oscar", "peggy", "trent", "victor", "walter",
]

LAST_NAMES = [
    "nguyen", "tran", "le", "pham", "hoang", "phan", "vu", "do", "bui", "dang",
    "smith", "johnson", "patel", "garcia", "lopez", "martin", "okonkwo",
]

DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "protonmail.com", "hotmail.com",
    "icloud.com", "mailinator.com", "tempmail.com",  # last two skew fraud
]

MERCHANTS = [
    "m_amzn_us", "m_ebay_us", "m_apple_us", "m_walmart_us", "m_target_us",
    "m_uber_us", "m_door_us", "m_stripe_test", "m_suspicious_1", "m_suspicious_2",
]

MCC_CODES = ["5411", "5942", "5812", "5732", "4121", "5999", "7011", "4814"]

CHANNELS = ["ecommerce", "card_present", "mobile", "transfer"]

COUNTRIES = ["US", "GB", "DE", "FR", "JP", "VN", "BR", "IN", "NG", "RU"]


def make_user_pool(n: int, rng: random.Random) -> list[dict]:
    pool = []
    for i in range(n):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        domain = rng.choice(DOMAINS)
        pool.append({
            "user_id": hmac_hash(f"{first}.{last}.{i}", load_settings().pii_hmac_key),
            "device_fp": hmac_hash(f"dev-{i}", load_settings().pii_hmac_key),
            "email_domain_hash": hmac_hash(domain, load_settings().pii_hmac_key),
            "country": rng.choice(COUNTRIES),
            "home_lat": rng.uniform(-60, 70),
            "home_lon": rng.uniform(-170, 170),
        })
    return pool


def synth_row(user: dict, ts: datetime, rng: random.Random, *, fraud: bool) -> dict:
    """Produce one canonical row.  ``fraud`` controls label + suspicious traits."""
    base_amount = rng.lognormvariate(4.5, 1.2)            # ~$90 median
    if fraud:
        amount = base_amount * rng.uniform(5, 25)          # fat amounts
        mcc = rng.choice(["7995", "6051", "4829"])          # gambling/quasi-cash
        merch = rng.choice([m for m in MERCHANTS if "susp" in m] or MERCHANTS)
        country = rng.choice(["NG", "RU", "BR"])
        device_age_hours = rng.uniform(0, 12)
    else:
        amount = base_amount
        mcc = rng.choice(MCC_CODES)
        merch = rng.choice(MERCHANTS[:6])
        country = user["country"]
        device_age_hours = rng.uniform(48, 2000)

    # light jitter around home location
    lat = user["home_lat"] + rng.gauss(0, 0.5)
    lon = user["home_lon"] + rng.gauss(0, 0.5)

    return {
        "tx_id":             new_ulid(),
        "user_id":           user["user_id"],
        "dataset_source":    "synthetic",
        "schema_version":    1,
        "ts_ms":             int(ts.timestamp() * 1000),
        "amount_minor":      int(round(amount * 100)),
        "currency":          "USD",
        "channel":           rng.choice(CHANNELS) if not fraud else "ecommerce",
        "device_fp":         user["device_fp"],
        "ip_hash":           hmac_hash(f"ip-{user['user_id'][-6:]}", load_settings().pii_hmac_key),
        "email_domain_hash": user["email_domain_hash"],
        "card_bin":          f"{rng.randint(400000, 555555)}",
        "merchant_id":       merch,
        "mcc":               mcc,
        "country":           country,
        "ip_country":        country,
        "lat":               round(max(min(lat, 90), -90), 4),
        "lon":               round(max(min(lon, 180), -180), 4),
        "label":             1 if fraud else 0,
        # Some dataset-specific features that will be filtered by the schema.
        "device_age_hours":  round(device_age_hours, 2),
        "velocity_1h":       rng.randint(0, 4) if not fraud else rng.randint(6, 20),
    }


def generate(n_users: int = 200, n_rows: int = 1_000, fraud_ratio: float = 0.04, seed: int = 42) -> pd.DataFrame:
    rng = random.Random(seed)
    users = make_user_pool(n_users, rng)

    # Span 30 days ending "now"
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(days=30)

    rows = []
    for _ in range(n_rows):
        ts = start + timedelta(seconds=rng.uniform(0, (end - start).total_seconds()))
        user = rng.choice(users)
        is_fraud = rng.random() < fraud_ratio
        rows.append(synth_row(user, ts, rng, fraud=is_fraud))

    df = pd.DataFrame(rows)
    df = df.sort_values("ts_ms").reset_index(drop=True)
    # `label` must be nullable Int8 to survive pandera validation when an
    # entire column happens to be present-but-not-NA.
    df["label"] = df["label"].astype("Int8")
    return df


# --- CLI ---------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="Generate synthetic canonical dataset.")
    p.add_argument("--users", type=int, default=200)
    p.add_argument("--rows", type=int, default=1_000)
    p.add_argument("--fraud-ratio", type=float, default=0.04)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", type=Path, default=Path("data/canonical/sample.parquet"))
    args = p.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating {args.rows} rows for {args.users} users (fraud={args.fraud_ratio:.2%})…")
    df = generate(args.users, args.rows, args.fraud_ratio, args.seed)
    print(f"  before validation: rows={len(df):,}")

    validated = validate_canonical_df(df)
    print(f"  after  validation: rows={len(validated):,}")

    # Make label properly nullable for parquet round-trip
    validated["label"] = validated["label"].astype("Int8")

    validated.to_parquet(args.out, engine="pyarrow", index=False)
    print(f"✓ Wrote {args.out} ({args.out.stat().st_size / 1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())