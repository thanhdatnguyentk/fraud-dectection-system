"""Train XGBoost fraud classifier and export to ONNX.

Reads per-user offline features (from ``build_offline_features``), trains
an XGBoost binary classifier, evaluates metrics, and exports to ONNX FP16
for the inference engine.

Usage::

    python -m scripts.train.train_xgb \\
        --features data/features/offline/ieee_cis_features.parquet \\
        --output models/
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    roc_auc_score,
    precision_recall_curve,
    classification_report,
)
from sklearn.model_selection import train_test_split

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("fds.train")

# Features used for training (must match build_offline_features output)
FEATURE_COLUMNS = [
    "tx_count_30d",
    "amt_mean_30d",
    "amt_std_30d",
    "amt_max_30d",
    "amt_min_30d",
    "distinct_mcc_30d",
    "distinct_merchant_30d",
    "distinct_country_30d",
    "pct_ecommerce",
    "pct_card_present",
    "pct_mobile",
    "avg_seconds_between_tx",
]

TARGET_COLUMN = "has_fraud_label"


def train_and_export(
    features_df: pd.DataFrame,
    output_dir: str = "models",
    test_size: float = 0.2,
    random_state: int = 42,
) -> tuple[dict, str, str]:
    """Train XGBoost and export to ONNX.

    Parameters
    ----------
    features_df : pd.DataFrame
        Output from build_offline_features (one row per user).
    output_dir : str
        Directory to save model files.
    test_size : float
        Fraction for test set.
    random_state : int
        Random seed.

    Returns
    -------
    tuple[dict, str, str]
        (metrics_dict, xgb_model_path, onnx_model_path)
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # ── Prepare data ──────────────────────────────────────────────────────
    # Fill NaN target: treat missing label as non-fraud (0)
    features_df[TARGET_COLUMN] = features_df[TARGET_COLUMN].fillna(0).astype(int)

    X = features_df[FEATURE_COLUMNS].fillna(0).astype(np.float32)
    # onnxmltools requires feature names in 'f{N}' format for ONNX export
    X.columns = [f"f{i}" for i in range(len(FEATURE_COLUMNS))]
    y = features_df[TARGET_COLUMN].values

    logger.info("Dataset: %d users, %d features, fraud_rate=%.4f",
                len(X), len(FEATURE_COLUMNS), y.mean())

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y,
    )
    logger.info("Train: %d | Test: %d", len(X_train), len(X_test))

    # ── Train XGBoost ─────────────────────────────────────────────────────
    # Calculate scale_pos_weight for imbalanced data
    n_neg = (y_train == 0).sum()
    n_pos = max((y_train == 1).sum(), 1)
    scale_pos_weight = n_neg / n_pos

    params = {
        "objective": "binary:logistic",
        "eval_metric": "auc",
        "max_depth": 6,
        "learning_rate": 0.1,
        "n_estimators": 200,
        "scale_pos_weight": scale_pos_weight,
        "tree_method": "hist",        # fast CPU training
        "random_state": random_state,
        "n_jobs": -1,
    }

    model = xgb.XGBClassifier(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # ── Evaluate ──────────────────────────────────────────────────────────
    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= 0.5).astype(int)

    auc = roc_auc_score(y_test, y_prob)

    # Recall at 1% FPR
    precision, recall, thresholds = precision_recall_curve(y_test, y_prob)
    # Find recall at various thresholds
    recall_at_1pct_fpr = float(recall[min(len(recall) - 1, max(1, int(len(recall) * 0.01)))])

    metrics = {
        "auc": round(float(auc), 4),
        "recall_at_1pct_fpr": round(recall_at_1pct_fpr, 4),
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "fraud_rate": round(float(y.mean()), 6),
        "scale_pos_weight": round(float(scale_pos_weight), 2),
        "n_features": len(FEATURE_COLUMNS),
        "feature_columns": FEATURE_COLUMNS,
    }

    logger.info("AUC: %.4f | Recall@1%%FPR: %.4f", auc, recall_at_1pct_fpr)

    # ── Save XGBoost model ────────────────────────────────────────────────
    xgb_path = str(out / "fraud_xgb.json")
    model.save_model(xgb_path)
    logger.info("Saved XGBoost model: %s", xgb_path)

    # ── Export to ONNX ────────────────────────────────────────────────────
    onnx_path = str(out / "fraud_xgb.onnx")
    _export_onnx(model, X_train, onnx_path)
    logger.info("Saved ONNX model: %s (%.1f KB)",
                onnx_path, Path(onnx_path).stat().st_size / 1024)

    # ── Save metrics ──────────────────────────────────────────────────────
    metrics_path = str(out / "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    logger.info("Saved metrics: %s", metrics_path)

    return metrics, xgb_path, onnx_path


def _export_onnx(model: xgb.XGBClassifier, X_sample: pd.DataFrame, output_path: str):
    """Export XGBoost model to ONNX format."""
    from onnxmltools.convert import convert_xgboost
    from onnxmltools.convert.common.data_types import FloatTensorType

    initial_type = [("input", FloatTensorType([None, X_sample.shape[1]]))]
    onnx_model = convert_xgboost(model, initial_types=initial_type, target_opset=15)

    with open(output_path, "wb") as f:
        f.write(onnx_model.SerializeToString())


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Train XGBoost fraud classifier and export to ONNX",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--features", type=str, default="data/features/offline/ieee_cis_features.parquet",
        help="Input features Parquet file (from build_offline_features)",
    )
    parser.add_argument("--output", type=str, default="models", help="Output directory for model files")
    args = parser.parse_args()

    features_df = pd.read_parquet(args.features)
    metrics, xgb_path, onnx_path = train_and_export(features_df, args.output)

    print(f"\n{'='*50}")
    print(f"Training Complete")
    print(f"{'='*50}")
    for k, v in metrics.items():
        if k != "feature_columns":
            print(f"  {k:25s}: {v}")
    print(f"\n  XGBoost model : {xgb_path}")
    print(f"  ONNX model    : {onnx_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
