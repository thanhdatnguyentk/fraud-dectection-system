"""Unit tests for XGBoost training and ONNX export (Phase 3).

Uses the synthetic sample dataset to test the full training pipeline:
generate features → train XGBoost → export ONNX → verify inference.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(scope="module")
def sample_parquet(tmp_path_factory) -> Path:
    from scripts.synth.generate_sample import generate
    out = tmp_path_factory.mktemp("data") / "sample.parquet"
    df = generate(n_users=50, n_rows=500, seed=99)
    df.to_parquet(out, engine="pyarrow", index=False)
    return out


@pytest.fixture(scope="module")
def features_df(sample_parquet) -> pd.DataFrame:
    from scripts.feature.build_offline_features import build_features
    return build_features(str(sample_parquet))


@pytest.fixture(scope="module")
def train_result(features_df, tmp_path_factory):
    from scripts.train.train_xgb import train_and_export
    out_dir = tmp_path_factory.mktemp("model")
    return train_and_export(features_df, output_dir=str(out_dir))


class TestTrainXGBoost:
    """Test XGBoost training pipeline."""

    def test_returns_metrics_dict(self, train_result):
        metrics, model_path, onnx_path = train_result
        assert isinstance(metrics, dict)
        assert "auc" in metrics
        assert "recall_at_1pct_fpr" in metrics

    def test_auc_above_minimum(self, train_result):
        metrics, _, _ = train_result
        # On synthetic data, we expect at least 0.6 AUC (low bar for tiny data)
        assert metrics["auc"] >= 0.55, f"AUC too low: {metrics['auc']}"

    def test_model_file_exists(self, train_result):
        _, model_path, _ = train_result
        assert Path(model_path).exists()
        assert Path(model_path).stat().st_size > 0

    def test_onnx_file_exists(self, train_result):
        _, _, onnx_path = train_result
        assert Path(onnx_path).exists()
        assert Path(onnx_path).stat().st_size > 0

    def test_onnx_inference_produces_scores(self, train_result, features_df):
        import onnxruntime as ort
        _, _, onnx_path = train_result
        from scripts.train.train_xgb import FEATURE_COLUMNS
        sess = ort.InferenceSession(onnx_path)
        # Take 5 rows for inference
        X = features_df[FEATURE_COLUMNS].head(5).values.astype(np.float32)
        input_name = sess.get_inputs()[0].name
        result = sess.run(None, {input_name: X})
        # Should return probabilities
        probs = result[1]  # index 1 = probabilities for XGBoost
        assert probs.shape[0] == 5
        # Probabilities should be in [0, 1]
        for row in probs:
            fraud_prob = row[1] if len(row) > 1 else row[0]
            assert 0 <= fraud_prob <= 1, f"Invalid probability: {fraud_prob}"
