"""End-to-end pipeline test: synthetic → canonical → validate → parquet round-trip.

This is the "it all works" smoke test.  It does NOT touch Kaggle or real CSVs.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from scripts.canonical_schema import quick_summary, validate_canonical_df


@pytest.fixture(scope="module")
def generated_parquet(tmp_path_factory) -> Path:
    """Run the generator and return the path of the produced parquet."""
    out = Path("data/canonical/_e2e_sample.parquet")
    result = subprocess.run(
        [sys.executable, "-m", "scripts.synth.generate_sample",
         "--rows", "500", "--users", "50", "--out", str(out)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"generator failed:\n{result.stderr}")
    return out


def test_e2e_round_trip(generated_parquet):
    # 1. parquet readable
    df = pd.read_parquet(generated_parquet)
    assert len(df) > 0

    # 2. still passes canonical schema
    validate_canonical_df(df)

    # 3. summary stats look reasonable
    s = quick_summary(df)
    assert s["rows"] == 500
    assert 0.0 <= (s["fraud_rows"] or 0) / s["rows"] <= 0.10
    assert s["distinct_user_id"] > 0
    assert s["min_ts_ms"] is not None and s["max_ts_ms"] > s["min_ts_ms"]


def test_generated_parquet_has_all_canonical_columns(generated_parquet):
    df = pd.read_parquet(generated_parquet)
    required = {
        "tx_id", "user_id", "dataset_source", "schema_version", "ts_ms",
        "amount_minor", "currency", "channel", "label",
    }
    assert required.issubset(set(df.columns))


def test_dataset_source_is_synthetic(generated_parquet):
    df = pd.read_parquet(generated_parquet)
    assert (df["dataset_source"] == "synthetic").all()


def test_label_is_binary(generated_parquet):
    df = pd.read_parquet(generated_parquet)
    assert set(df["label"].dropna().unique()).issubset({0, 1})


def test_user_ids_are_64_char_hex(generated_parquet):
    df = pd.read_parquet(generated_parquet)
    sample = df["user_id"].dropna().iloc[0]
    assert len(sample) == 64
    assert all(c in "0123456789abcdef" for c in sample)
