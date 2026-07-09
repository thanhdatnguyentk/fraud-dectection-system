"""Canonical schema for the Fraud Detection System.

This module defines `tx_canonical.v1` — the unified schema every dataset
(IEEE-CIS, ULB, Sparkov, PaySim) is mapped to before entering the pipeline.

Why a schema-first contract?
  * Every downstream component (Kafka producer, Flink, Redis, Triton, ClickHouse)
    reads the same column names & types.
  * Drift in raw data fails fast at ingestion (pandera `Check`s).
  * New datasets (or new fields) bump `schema_version` and we evolve explicitly.

Usage:
    from scripts.canonical_schema import CANONICAL_SCHEMA, validate_canonical_df
    validate_canonical_df(df)        # raises SchemaError on violation
"""
from __future__ import annotations

import re
from typing import Final

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Field

# =============================================================================
# Constants
# =============================================================================

SCHEMA_VERSION: Final[int] = 1
SCHEMA_NAME: Final[str] = "tx_canonical"

ALLOWED_SOURCES: Final[tuple[str, ...]] = (
    "ieee_cis",
    "ulb",
    "sparkov",
    "paysim",
    "synthetic",
)

ALLOWED_CHANNELS: Final[tuple[str, ...]] = (
    "card_present",
    "ecommerce",
    "mobile",
    "transfer",
    "atm",
    "other",
)

# Hex-encoded HMAC-SHA256 digest is exactly 64 lowercase hex chars.
HEX64 = r"^[a-f0-9]{64}$"

# ULID: 26 chars Crockford-base32.
ULID = r"^[0-9A-HJKMNP-TV-Z]{26}$"

# =============================================================================
# Pandera schema
# =============================================================================

CANONICAL_SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    name=SCHEMA_NAME,
    # Columns may appear in any order; required ones must exist.
    columns={
        # --- Identity / keys ---
        "tx_id":             pa.Column(str,  Check.str_matches(ULID),  nullable=False, unique=True),
        "user_id":           pa.Column(str,  Check.str_matches(HEX64),  nullable=False),
        "dataset_source":    pa.Column(str,  Check.isin(ALLOWED_SOURCES), nullable=False),
        "schema_version":    pa.Column(int,  Check.equal_to(SCHEMA_VERSION), nullable=False),

        # --- Time ---
        "ts_ms":             pa.Column("int64", Check.ge(0), nullable=False),

        # --- Money ---
        "amount_minor":      pa.Column("int64", Check.ge(0), nullable=False),
        "currency":          pa.Column(str,  Check.str_length(3, 3), nullable=False),

        # --- Channel / merchant / network ---
        "channel":           pa.Column(str,  Check.isin(ALLOWED_CHANNELS), nullable=False),

        # --- Optional but typed fields (allow null where appropriate) ---
        "device_fp":         pa.Column(str,  Check.str_matches(HEX64),  nullable=True),
        "ip_hash":           pa.Column(str,  Check.str_matches(HEX64),  nullable=True),
        "email_domain_hash": pa.Column(str,  Check.str_matches(HEX64),  nullable=True),
        "card_bin":          pa.Column(str,  nullable=True),
        "merchant_id":       pa.Column(str,  nullable=True),
        "mcc":               pa.Column(str,  nullable=True),
        "country":           pa.Column(str,  Check.str_length(2, 2), nullable=True),
        "ip_country":        pa.Column(str,  Check.str_length(2, 2), nullable=True),
        "lat":               pa.Column("float64", Check.between(-90, 90), nullable=True),
        "lon":               pa.Column("float64", Check.between(-180, 180), nullable=True),

        # --- Label (0/1) and attributes (free-form) ---
        # Use pandas' nullable Int8 so missing values are stored as <NA>, not NaN.
        "label":             pa.Column("Int8", Check.isin([0, 1, -1]), nullable=True),
        "attributes":        pa.Column(object, nullable=True),
    },
    # Allow extra columns for dataset-specific feature dumps, but warn in CI.
    strict="filter",
    ordered=False,
    unique_column_names=True,
    coerce=False,
    # Allow datasets that don't have every optional column to be filled with NaN/None.
    add_missing_columns=True,
)


# =============================================================================
# Public helpers
# =============================================================================

def validate_canonical_df(df: pd.DataFrame, *, lazy: bool = True) -> pd.DataFrame:
    """Validate ``df`` against `CANONICAL_SCHEMA` and return the validated frame.

    Parameters
    ----------
    df : pd.DataFrame
        The frame produced by one of the canonicalization scripts.
    lazy : bool, default True
        Collect ALL schema errors before raising.  Useful when you want to see
        every defect at once instead of fixing them one-by-one.

    Raises
    ------
    pandera.errors.SchemaError, pandera.errors.SchemaErrors
    """
    return CANONICAL_SCHEMA.validate(df, lazy=lazy)


def quick_summary(df: pd.DataFrame) -> dict:
    """Lightweight stats useful for CLI smoke-tests and CI assertions."""
    return {
        "rows": int(len(df)),
        "distinct_user_id": int(df["user_id"].nunique()) if "user_id" in df else 0,
        "fraud_rows": int(df["label"].sum()) if "label" in df else None,
        "sources": df["dataset_source"].value_counts().to_dict() if "dataset_source" in df else {},
        "min_ts_ms": int(df["ts_ms"].min()) if "ts_ms" in df else None,
        "max_ts_ms": int(df["ts_ms"].max()) if "ts_ms" in df else None,
    }


__all__ = [
    "SCHEMA_VERSION",
    "SCHEMA_NAME",
    "ALLOWED_SOURCES",
    "ALLOWED_CHANNELS",
    "CANONICAL_SCHEMA",
    "validate_canonical_df",
    "quick_summary",
]