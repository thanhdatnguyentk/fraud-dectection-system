"""Streamlit Dashboard for Fraud Detection System (Local Edition).

Monitors Redis feature store, Kafka producer status, and model training metrics.

Usage::

    cd /home/huy/dat/fraud-dectection/fraud-dectection-system
    source .venv/bin/activate
    streamlit run scripts/dashboard/app.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import redis
import streamlit as st


st.set_page_config(
    page_title="FDS Dashboard",
    page_icon="🛡️",
    layout="wide",
)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def get_redis_client() -> redis.Redis | None:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        return r
    except redis.ConnectionError:
        return None


def load_model_metrics() -> dict | None:
    metrics_path = Path("models/metrics.json")
    if metrics_path.exists():
        with open(metrics_path, "r") as f:
            return json.load(f)
    return None


def fetch_redis_users(r: redis.Redis, limit: int = 100) -> pd.DataFrame:
    keys = r.keys("feat:user:*")
    if not keys:
        return pd.DataFrame()
    
    # Just take a sample to avoid loading too much
    sample_keys = keys[:limit]
    
    pipe = r.pipeline()
    for k in sample_keys:
        pipe.hgetall(k)
    results = pipe.execute()
    
    data = []
    for k, v in zip(sample_keys, results):
        row = {"user_id": k.replace("feat:user:", "")}
        row.update(v)
        data.append(row)
        
    df = pd.DataFrame(data)
    # Convert numeric columns
    numeric_cols = ["tx_count_10m", "amt_sum_1h", "max_amt_1h", "distinct_mcc_1h", "seconds_since_last_tx"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
    return df


def main():
    st.title("🛡️ Fraud Detection System Dashboard")
    st.markdown("Real-time monitoring for the local FDS pipeline.")
    
    r = get_redis_client()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Infrastructure")
        if r:
            st.success("✅ Redis is connected")
            total_users = len(r.keys("feat:user:*"))
            st.metric("Users in Feature Store", f"{total_users:,}")
        else:
            st.error("❌ Redis is disconnected. Ensure docker compose is running.")
            
    with col2:
        st.subheader("Streaming Metrics (Phase 2)")
        if r:
            df_users = fetch_redis_users(r, limit=1000)
            if not df_users.empty:
                st.metric("Total tx_count_10m (Sample)", f"{df_users['tx_count_10m'].sum():,.0f}")
                st.metric("Total amt_sum_1h (Sample)", f"${df_users['amt_sum_1h'].sum() / 100:,.2f}")
            else:
                st.info("No feature data found in Redis yet.")
                
    with col3:
        st.subheader("Model Metrics (Phase 3)")
        metrics = load_model_metrics()
        if metrics:
            st.success("✅ XGBoost Model Trained")
            st.metric("AUC", metrics.get("auc", "N/A"))
            st.metric("Recall @ 1% FPR", metrics.get("recall_at_1pct_fpr", "N/A"))
            st.caption(f"Trained on {metrics.get('train_size', 0):,} rows")
        else:
            st.warning("⚠️ Model not trained yet. Run `train_xgb` script.")

    st.divider()
    
    st.subheader("Real-time User Features (Sample)")
    if r and not df_users.empty:
        st.dataframe(df_users, use_container_width=True)
        
        st.subheader("Feature Distributions")
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            if "tx_count_10m" in df_users.columns:
                st.bar_chart(df_users["tx_count_10m"].value_counts())
        with col_chart2:
            if "amt_sum_1h" in df_users.columns:
                st.line_chart(df_users["amt_sum_1h"].sort_values(ascending=False).reset_index(drop=True))


if __name__ == "__main__":
    main()
