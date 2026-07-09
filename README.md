# Fraud Detection System (Local Personal Edition)

> **Status:** Giai đoạn 1 của kế hoạch triển khai (Local Edition) trong [`plans/local-implementation-plan.md`](plans/local-implementation-plan.md).
> Hạ tầng cục bộ (Redpanda, Redis, Postgres) đã được thiết lập qua Docker Compose.

## Quick start

```bash
# 1. Khởi động hạ tầng (Redpanda, Redis, Postgres)
docker compose up -d

# 2. install deps (sử dụng uv để cài đặt siêu tốc)
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt

# 3. run the test suite (33 tests, ~1s)
python -m pytest tests/ -v

# 3. generate a synthetic sample (no Kaggle creds needed)
python -m scripts.synth.generate_sample --rows 1000 --out data/canonical/sample.parquet

# 4. download real datasets (requires KAGGLE_USERNAME/KAGGLE_KEY env)
python -m scripts.ingest.download_datasets --all
python -m scripts.ingest.download_datasets --datasets sparkov   # if creds missing: error
```

## Project layout

```
fraud-dectection-system/
├── data/
│   ├── raw/          # gitignored: CSVs from Kaggle/GitHub
│   ├── canonical/    # gitignored: tx_canonical.v1 parquets
│   └── artifacts/    # gitignored: model files, reports
├── plans/
│   ├── fraud-detection-system-design.md   # full system architecture (Local optimized)
│   ├── data-synthesis-plan.md             # data strategy & plan
│   └── local-implementation-plan.md       # kế hoạch triển khai 5 giai đoạn
├── scripts/
│   ├── canonical_schema.py                # single source of truth: tx_canonical.v1
│   ├── common.py                          # settings, HMAC, ULID
│   ├── ingest/
│   │   ├── download_datasets.py           # tải 4 dataset (Kaggle + Sparkov Kaggle mirror)
│   │   ├── canonicalize_ieee_cis.py
│   │   ├── canonicalize_ulb.py
│   │   ├── canonicalize_sparkov.py
│   │   └── canonicalize_paysim.py
│   ├── synth/
│   │   └── generate_sample.py             # sinh data mẫu khi chưa có creds
│   ├── feature/                           # (W3-W4) offline features
│   └── train/                             # (W5+) training
├── pipelines/                             # (W12) Airflow/Dagster DAGs
└── tests/
    ├── test_canonical_schema.py           # 10 tests: pandera rules
    ├── test_common.py                     # 8 tests: hashing, ULID
    ├── test_data_quality.py               # 6 tests
    ├── test_e2e_pipeline.py               # 5 tests: round-trip
    └── test_scenarios.py                  # 4 tests: streaming scenarios
```

## Canonical schema (tx_canonical.v1)

Single contract for every dataset — defined in [`scripts/canonical_schema.py`](scripts/canonical_schema.py).

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `tx_id` | string (ULID) | ✅ | Unique per transaction |
| `user_id` | string (64-hex) | ✅ | HMAC-SHA256 of PII |
| `dataset_source` | enum | ✅ | `ieee_cis` / `ulb` / `sparkov` / `paysim` / `synthetic` |
| `schema_version` | int | ✅ | Currently `1` |
| `ts_ms` | int64 | ✅ | Epoch milliseconds |
| `amount_minor` | int64 | ✅ | Amount × 100 (no floating point in money) |
| `currency` | string(3) | ✅ | ISO 4217 |
| `channel` | enum | ✅ | `card_present` / `ecommerce` / `mobile` / `transfer` / `atm` / `other` |
| `device_fp` | string(64-hex) | ⚪ | Hash of device signature |
| `ip_hash` | string(64-hex) | ⚪ | Hash of IP |
| `email_domain_hash` | string(64-hex) | ⚪ | Hash of email domain |
| `card_bin` | string | ⚪ | First 6 digits |
| `merchant_id` | string | ⚪ | Tokenized |
| `mcc` | string | ⚪ | Merchant category code |
| `country` | string(2) | ⚪ | ISO 3166-1 alpha-2 |
| `ip_country` | string(2) | ⚪ | |
| `lat`, `lon` | float | ⚪ | |
| `label` | Int8 | ⚪ | 0 / 1 / -1 (unknown) |
| `attributes` | object | ⚪ | Per-dataset extra columns, dropped in strict mode |

## Roadmap (Local Edition)

See [`plans/local-implementation-plan.md`](plans/local-implementation-plan.md) cho chi tiết 5 giai đoạn.

- ✅ **Giai đoạn 1**: Khởi tạo Hạ tầng Cục bộ (Redpanda, Redis, Postgres bằng Docker Compose)
- ⏭ **Giai đoạn 2**: Xây dựng Ingestion & Stream Processing (Pipeline) với Redpanda & Faust/Quix
- ⏭ **Giai đoạn 3**: Feature Engineering Offline (Polars/DuckDB) & Model Training (XGBoost ONNX)
- ⏭ **Giai đoạn 4**: Xây dựng Ultra-Fast Python API (Scoring Engine với FastAPI)
- ⏭ **Giai đoạn 5**: Tích hợp, Load Test & Báo cáo

## Environment

Copy `.env.example` to `.env` and fill in real values. The repo works without
`.env` using dev defaults, but production deployments must set:

- `PII_HMAC_KEY` — 64-char hex secret for HMAC-SHA256 of PII fields
- `IEEE_CIS_EPOCH` — reference timestamp for IEEE-CIS `TransactionDT`
- `KAGGLE_USERNAME` / `KAGGLE_KEY` — for downloading real datasets

## License

See `LICENSE`.
