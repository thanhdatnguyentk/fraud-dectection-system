"""Tests for `scripts.canonical_schema`."""
from __future__ import annotations

import pandas as pd
import pandera.errors as pe
import pytest

from scripts.canonical_schema import (
    CANONICAL_SCHEMA,
    SCHEMA_VERSION,
    validate_canonical_df,
    quick_summary,
)


def _base_row(**overrides) -> dict:
    row = {
        "tx_id": "01J7F2A4QX9D2PE5K7HBN3M2RX",  # valid ULID
        "user_id": "a" * 64,                    # 64-char hex
        "dataset_source": "synthetic",
        "schema_version": SCHEMA_VERSION,
        "ts_ms": 1_720_515_262_123,
        "amount_minor": 12_999,
        "currency": "USD",
        "channel": "ecommerce",
        # optionals default to NaN/null
    }
    row.update(overrides)
    return row


def test_minimal_valid_row_passes():
    df = pd.DataFrame([_base_row()])
    validated = validate_canonical_df(df)
    assert len(validated) == 1


def test_missing_required_field_fails():
    df = pd.DataFrame([_base_row()])  # has all fields
    df = df.drop(columns=["amount_minor"])
    with pytest.raises(pe.SchemaErrors):
        validate_canonical_df(df)


def test_bad_currency_length_fails():
    df = pd.DataFrame([_base_row(currency="USDD")])
    with pytest.raises(pe.SchemaErrors):
        validate_canonical_df(df)


def test_user_id_must_be_hex64():
    df = pd.DataFrame([_base_row(user_id="not_hex_at_all_just_a_string_to_fail")])
    with pytest.raises(pe.SchemaErrors):
        validate_canonical_df(df)


def test_dataset_source_must_be_allowed():
    df = pd.DataFrame([_base_row(dataset_source="made_up_source")])
    with pytest.raises(pe.SchemaErrors):
        validate_canonical_df(df)


def test_label_must_be_0_1_or_minus_1():
    df = pd.DataFrame([_base_row(label=2)])
    with pytest.raises(pe.SchemaErrors):
        validate_canonical_df(df)


def test_schema_version_must_be_1():
    df = pd.DataFrame([_base_row(schema_version=2)])
    with pytest.raises(pe.SchemaErrors):
        validate_canonical_df(df)


def test_latitude_longitude_bounds():
    df = pd.DataFrame([_base_row(lat=999, lon=10)])
    with pytest.raises(pe.SchemaErrors):
        validate_canonical_df(df)


def test_quick_summary():
    df = pd.DataFrame([
        _base_row(tx_id="01J7F2A4QX9D2PE5K7HBN3M2RX"),
        _base_row(tx_id="01J7F2A4QX9D2PE5K7HBN3M2RS", label=1),
    ])
    s = quick_summary(df)
    assert s["rows"] == 2
    assert s["fraud_rows"] == 1
    assert s["sources"] == {"synthetic": 2}


def test_strict_mode_drops_unknown_columns():
    """strict='filter' silently drops unknown columns."""
    df = pd.DataFrame([_base_row()])
    df["unknown_extra"] = "ignored"
    out = validate_canonical_df(df)
    assert "unknown_extra" not in out.columns