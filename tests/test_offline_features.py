"""Unit tests for offline feature engineering (Phase 3).

Tests the DuckDB-based feature builder on the synthetic sample dataset.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from scripts.synth.generate_sample import generate


@pytest.fixture(scope="module")
def sample_parquet(tmp_path_factory) -> Path:
    """Generate a small synthetic dataset and save as Parquet."""
    out = tmp_path_factory.mktemp("data") / "sample.parquet"
    df = generate(n_users=50, n_rows=500, seed=99)
    df.to_parquet(out, engine="pyarrow", index=False)
    return out


class TestBuildOfflineFeatures:
    """Test that build_offline_features produces correct feature columns."""

    def test_output_has_required_feature_columns(self, sample_parquet):
        from scripts.feature.build_offline_features import build_features
        result = build_features(str(sample_parquet))
        required = [
            "user_id", "tx_count_30d", "amt_mean_30d", "amt_std_30d",
            "amt_max_30d", "distinct_mcc_30d", "distinct_merchant_30d",
            "fraud_rate_user", "pct_ecommerce",
        ]
        for col in required:
            assert col in result.columns, f"Missing column: {col}"

    def test_output_row_count_matches_distinct_users(self, sample_parquet):
        from scripts.feature.build_offline_features import build_features
        df_in = pd.read_parquet(sample_parquet)
        result = build_features(str(sample_parquet))
        assert len(result) == df_in["user_id"].nunique()

    def test_no_null_user_ids(self, sample_parquet):
        from scripts.feature.build_offline_features import build_features
        result = build_features(str(sample_parquet))
        assert result["user_id"].isna().sum() == 0

    def test_fraud_rate_between_0_and_1(self, sample_parquet):
        from scripts.feature.build_offline_features import build_features
        result = build_features(str(sample_parquet))
        assert (result["fraud_rate_user"] >= 0).all()
        assert (result["fraud_rate_user"] <= 1).all()

    def test_amounts_are_non_negative(self, sample_parquet):
        from scripts.feature.build_offline_features import build_features
        result = build_features(str(sample_parquet))
        assert (result["amt_mean_30d"] >= 0).all()
        assert (result["amt_max_30d"] >= 0).all()

    def test_can_save_and_reload_parquet(self, sample_parquet, tmp_path):
        from scripts.feature.build_offline_features import build_features
        result = build_features(str(sample_parquet))
        out_path = tmp_path / "features.parquet"
        result.to_parquet(out_path, index=False)
        reloaded = pd.read_parquet(out_path)
        assert len(reloaded) == len(result)
        assert list(reloaded.columns) == list(result.columns)
