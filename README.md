# Hệ thống Phát hiện Gian lận Thời gian Thực (Local Edition)
# Real-Time Fraud Detection System

> **Phiên bản:** 5/5 giai đoạn — đã vận hành end-to-end trên một máy
> **Ngày cập nhật:** 2026-07-09
> **Mục tiêu:** Toàn bộ pipeline phát hiện gian lận chạy trên 1 máy cá nhân (16 GB RAM, GTX 3050), đảm bảo **p99 ≤ 50 ms**.
> **Kiến trúc chi tiết:** xem [`plans/fraud-detection-system-design.md`](plans/fraud-detection-system-design.md) và [`plans/local-implementation-plan.md`](plans/local-implementation-plan.md).

---

## 🎯 Tổng quan

Hệ thống thực hiện đầy đủ luồng:

```
Producer (Parquet→Kafka) → Redpanda → Stream Processor (Quix Streams) 
    → Redis (sliding-window + offline) → FastAPI Scoring (ONNX) 
    → Rules Engine (simpleeval) → Decision (APPROVE/CHALLENGE/DECLINE)
```

Model hiện tại (XGBoost → ONNX FP16) đạt **AUC ≈ 0.81, recall@1%FPR ≈ 0.997** trên 13 553 user của IEEE-CIS.

---

## 🚀 Khởi động nhanh (Quick Start)

```bash
# 1. Khởi động hạ tầng (Redpanda, Redis, Postgres, Prometheus, Grafana)
docker compose up -d

# 2. Kích hoạt virtualenv (đã có sẵn trong repo)
source .venv/bin/activate

# 3. Cài dependencies (chỉ cần nếu môi trường mới)
uv pip install -r requirements.txt

# 4. Chạy bộ test (52 tests, ~1 giây)
python -m pytest tests/ -v

# 5. (Tuỳ chọn) Sinh dữ liệu mẫu nếu chưa có Kaggle credentials
python -m scripts.synth.generate_sample --rows 1000 --out data/canonical/sample.parquet

# 6. (Tuỳ chọn) Tải dataset thật — cần KAGGLE_USERNAME/KAGGLE_KEY
python -m scripts.ingest.download_datasets --all
```

---

## 🧭 Các lệnh thường dùng

### Lệnh test

```bash
pytest tests/ -v                                          # toàn bộ 52 tests
pytest tests/test_canonical_schema.py -v                  # 1 file
pytest tests/test_e2e_pipeline.py::test_e2e_round_trip -v # 1 test
pytest tests/ -k "fraud" -v                               # pattern match
```

### Pipeline dữ liệu (Phase 1)

```bash
# Tải dataset (cần credentials)
python -m scripts.ingest.download_datasets --all
python -m scripts.ingest.download_datasets --datasets sparkov

# Chuẩn hoá (canonicalize) — output Parquet vào data/canonical/
python -m scripts.ingest.canonicalize_ieee_cis --raw data/raw/ieee_cis --out data/canonical/ieee_cis.parquet
python -m scripts.ingest.canonicalize_ulb --raw data/raw/ulb/creditcard.csv --out data/canonical/ulb.parquet
python -m scripts.ingest.canonicalize_sparkov --raw data/raw/sparkov/fraudTrain.csv --out data/canonical/sparkov.parquet
python -m scripts.ingest.canonicalize_paysim --raw data/raw/paysim --out data/canonical/paysim.parquet

# Sinh dữ liệu synthetic (không cần Kaggle)
python -m scripts.synth.generate_sample --rows 1000 --out data/canonical/sample.parquet
```

### Feature engineering + Train (Phase 3)

```bash
# Tính offline features trên canonical Parquet (mỗi user 1 dòng)
python -m scripts.feature.build_offline_features \
    --input data/canonical/ieee_cis.parquet \
    --output data/features/offline/ieee_cis_features.parquet

# Backfill features vào Redis để cold-start cho user mới
python -m scripts.feature.backfill_redis \
    --input data/features/offline/ieee_cis_features.parquet

# Train XGBoost → export ONNX FP16 ra models/
python -m scripts.train.train_xgb \
    --features data/features/offline/ieee_cis_features.parquet \
    --output models/
```

### Streaming pipeline (Phase 2 — cần `docker compose up`)

```bash
# Producer: bắn Parquet vào Redpanda topic fds.tx.raw.v1
python -m scripts.synth.kafka_producer \
    --file data/canonical/sample.parquet --tps 200 --max 1000

# Stream processor: chạy nền, đọc Redpanda → Redis
python -m scripts.feature.stream_processor

# Kiểm thử các kịch bản tấn công
python -m scripts.synth.scenario_runner --scenario velocity_attack --tps 100
python -m scripts.synth.scenario_runner --scenario all --tps 200

# Smoke-test toàn pipeline (cần stream_processor đang chạy)
python -m scripts.synth.verify_pipeline
```

### Scoring API (Phase 4)

```bash
# Khởi FastAPI server (port 8001) với WebSocket Dashboard
python -m uvicorn scripts.api.main:app --host 0.0.0.0 --port 8001 --workers 1

# Test health
curl http://localhost:8001/health

# Gọi scoring
curl -X POST http://localhost:8001/api/v1/score -H "Content-Type: application/json" -d '{
    "tx_id": "01J7F2A4QX9D2PE5K7HBN3M2RX",
    "user_id": "u_8f12c9",
    "card_bin": "448588",
    "merchant_id": "m_amzn_us",
    "mcc": "5942",
    "channel": "ecommerce",
    "amount_minor": 12999,
    "currency": "USD",
    "country": "US",
    "ts_ms": 1720515262123
}'
```

### Load test + Dashboard (Phase 5)

```bash
# Load test API (5000 request, concurrency 100)
python -m scripts.tests.load_test \
    --target http://localhost:8001/api/v1/score \
    --requests 5000 --concurrency 100

# Red-team attacker (Chạy các kịch bản tấn công cơ bản)
python -m scripts.tests.red_team_attacker --mode all

# GAN-inspired Evasion Attacker (Máy chủ tấn công lách luật)
python -m scripts.tests.gan_evasion_server

# Mở Dashboard Real-time (WebSockets)
# Truy cập: http://localhost:8001/dashboard/index.html
```

### Hạ tầng

```bash
docker compose up -d    # Redpanda :19092, Redis :6379, Postgres :5432, Prometheus :9090, Grafana :3000
docker compose down -v  # xoá volumes
```

---

## 📐 Schema chuẩn — `tx_canonical.v1`

Mọi dataset (IEEE-CIS, ULB, Sparkov, PaySim, synthetic) **phải** được map về schema này trước khi đi tiếp. Định nghĩa tại `scripts/canonical_schema.py` (pandera) — enforce qua `validate_canonical_df(df)`.

| Trường | Kiểu | Bắt buộc | Mô tả |
|--------|------|----------|--------|
| `tx_id` | string (ULID 26 ký tự) | ✅ | Định danh duy nhất |
| `user_id` | string (64 hex) | ✅ | HMAC-SHA256 của PII |
| `dataset_source` | enum | ✅ | `ieee_cis` / `ulb` / `sparkov` / `paysim` / `synthetic` |
| `schema_version` | int | ✅ | Luôn là `1` |
| `ts_ms` | int64 | ✅ | Epoch milliseconds |
| `amount_minor` | int64 | ✅ | Số tiền × 100 (cents, không float) |
| `currency` | string(3) | ✅ | ISO 4217 |
| `channel` | enum | ✅ | `card_present` / `ecommerce` / `mobile` / `transfer` / `atm` / `other` |
| `device_fp` | string(64 hex) | ⚪ | Hash của thiết bị |
| `ip_hash` | string(64 hex) | ⚪ | Hash IP |
| `email_domain_hash` | string(64 hex) | ⚪ | Hash email domain |
| `card_bin` | string | ⚪ | 6 số đầu BIN |
| `merchant_id` | string | ⚪ | Tokenized |
| `mcc` | string | ⚪ | Merchant Category Code |
| `country` | string(2) | ⚪ | ISO-3166 alpha-2 |
| `ip_country` | string(2) | ⚪ | |
| `lat`, `lon` | float | ⚪ | |
| `label` | Int8 | ⚪ | 0 / 1 / -1 |
| `attributes` | object | ⚪ | Cột thừa của từng dataset |

---

## 🏗️ Kiến trúc tổng thể (5 giai đoạn)

### Giai đoạn 1 — Hạ tầng cục bộ ✅
- Docker Compose: Redpanda, Redis, Postgres, Prometheus, Grafana
- Một câu `docker compose up -d` để dựng toàn bộ.

### Giai đoạn 2 — Streaming Pipeline ✅
- `kafka_producer.py`: đẩy Parquet → Redpanda với `--tps` điều chỉnh (đã test 400+ TPS).
- `stream_processor.py`: Quix Streams consumer, sliding-window features qua Redis Sorted Set:
  - `sw:tx:10m:{user_id}` — 10 phút gần nhất
  - `sw:txdata:1h:{user_id}` — 1 giờ gần nhất
  - `feat:user:{user_id}` — Hash chứa feature đã tính (API đọc)
- `scenario_runner.py`: 6 kịch bản tấn công (velocity_attack, impossible_travel, device_spray, fat_finger, burst_spike, holiday_spike).

### Giai đoạn 3 — Offline Features + Training ✅
- `build_offline_features.py`: DuckDB SQL Window functions trên canonical Parquet → 12 features/user.
- `backfill_redis.py`: đẩy features vào `offline:user:{user_id}` Hash để cold-start.
- `train_xgb.py`: XGBoost → ONNX FP16 export vào `models/fraud_xgb.onnx`.

### Giai đoạn 4 — Scoring API ✅
- FastAPI + uvicorn, async pipeline.
- ONNX Runtime (CUDA EP trên GTX 3050).
- Singleflight chống bão request trùng `user_id`.
- Rules Engine bằng `simpleeval` — Hard rules (blacklist BIN, amount cap) + Soft rules (score + policy).
- Endpoints:
  - `GET  /health`
  - `POST /api/v1/score`

### Giai đoạn 5 — Load Test + Tích hợp ✅
- `load_test.py`: async load test, đo p50/p95/p99 latency.
- `red_team_attacker.py`: chạy các kịch bản adversarial tự động.
- `dashboard/app.py`: Streamlit dashboard cho Redis, Kafka, model metrics.

---

## 📂 Cấu trúc dự án

```
fraud-dectection-system/
├── data/                            # (gitignored)
│   ├── raw/                         #   CSV gốc từ Kaggle
│   ├── canonical/                   #   Parquet tx_canonical.v1
│   ├── features/offline/            #   Per-user features cho training
│   └── artifacts/                   #   Model artifacts + reports
├── models/                          # (gitignored) fraud_xgb.onnx, fraud_xgb.json, metrics.json
├── plans/
│   ├── fraud-detection-system-design.md   # kiến trúc tổng thể
│   ├── data-synthesis-plan.md             # chiến lược dữ liệu 12 tuần
│   └── local-implementation-plan.md       # 5 giai đoạn Local
├── scripts/
│   ├── canonical_schema.py          # single source of truth: tx_canonical.v1
│   ├── common.py                    # settings, HMAC, ULID
│   ├── ingest/
│   │   ├── download_datasets.py     # tải 4 dataset
│   │   └── canonicalize_*.py        # 4 script ánh xạ về canonical
│   ├── synth/
│   │   ├── generate_sample.py       # sinh dữ liệu mẫu
│   │   ├── kafka_producer.py        # Parquet → Redpanda
│   │   ├── scenario_runner.py       # kịch bản tấn công
│   │   └── verify_pipeline.py       # E2E smoke test
│   ├── feature/
│   │   ├── build_offline_features.py  # DuckDB offline features
│   │   ├── backfill_redis.py          # offline → Redis
│   │   └── stream_processor.py        # Quix Streams sliding-window
│   ├── train/
│   │   └── train_xgb.py             # XGBoost + ONNX export
│   ├── api/
│   │   └── main.py                  # FastAPI scoring endpoint
│   ├── dashboard/
│   │   └── app.py                   # Streamlit (Cũ)
│   ├── tests/
│   │   ├── load_test.py             # async load test API
│   │   ├── gan_evasion_server.py    # Máy chủ tấn công GAN-inspired
│   │   └── red_team_attacker.py     # adversarial test
├── dashboard_ui/
│   ├── index.html                   # HTML Dashboard mới
│   ├── style.css                    # CSS Dashboard
│   └── script.js                    # WebSockets Client cho Dashboard
├── tests/                           # pytest
│   ├── test_canonical_schema.py     # 10 tests
│   ├── test_common.py               # 8 tests
│   ├── test_data_quality.py         # 6 tests
│   ├── test_e2e_pipeline.py         # 5 tests
│   ├── test_scenarios.py            # 4 tests
│   ├── test_offline_features.py
│   ├── test_stream_processor.py
│   ├── test_train_xgb.py
│   └── test_api_scoring.py
├── docker-compose.yml               # Redpanda, Redis, Postgres, Prometheus, Grafana
├── ops/prometheus.yml
├── .env.example                     # mẫu biến môi trường
├── .gitignore
├── requirements.txt
├── README.md                        # file này
└── CLAUDE.md                        # hướng dẫn cho Claude Code
```

---

## 🔒 Bảo mật & Tuân thủ

- **PII tokenization ngay tại ingestion**: mọi trường nhạy cảm (`card1`, `email`, `device_fp`) đều được hash qua HMAC-SHA256 bằng key trong `PII_HMAC_KEY` (env, xoay vòng mỗi quý).
- **`tx_id` là ULID** (time-ordered, 26 char) — không lộ ID gốc.
- **Money ở integer cents** — không bao giờ float, không bao giờ string.
- **`kaggle.json` đã được gitignore** — đặt tại `~/.kaggle/kaggle.json` (chmod 600) hoặc dùng `KAGGLE_USERNAME`/`KAGGLE_KEY` env.

---

## ⚠️ Một số "cái bẫy" đã biết

| Bẫy | Cách tránh |
|-----|------------|
| `ieee-fraud-detection` slug gốc trả 403 | Dùng mirror `lixfemso/ieee-fraud-detection` |
| `addr1` trong IEEE-CIS không phải ISO-3166 | Để `country = NULL`, giữ zip vào `attributes.addr1_zip_prefix` |
| `pd.to_datetime(...).astype("int64") // 1_000_000` ở Sparkov | Phải là `// 1000` (pandas trả `datetime64[us, UTC]`, chia 1000 cho ms) |
| File PaySim có tên biến đổi (`PS_<digits>_log.csv`) | Truyền `--raw data/raw/paysim` (cả thư mục, không phải file) |
| `pandera` import path | Dùng `pandera.pandas as pa`, không `import pandera as pa` |
| `test_api_scoring.py` cần `httpx` | Cài thêm nếu collection error: `pip install httpx` |

---

## 📊 Quan sát & Giám sát

- **Prometheus**: scrape từ API + exporter của Redpanda/Redis tại `localhost:9090`.
- **Grafana**: dashboard mẫu tại `localhost:3000` (admin/admin).
- **Streamlit dashboard**: `streamlit run scripts/dashboard/app.py` — theo dõi Redis features, Kafka lag, model metrics.
- **Audit log**: ghi vào Postgres (`decision_audit` table) bởi API service.

---

## 🛣️ Lộ trình còn lại

Các hạng mục **chưa** triển khai (xem `CLAUDE.md` để biết chi tiết):

- Airflow/Dagster DAGs trong `pipelines/` (placeholder).
- Multi-model ensemble (Autoencoder + GNN từ `plans/fraud-detection-system-design.md`).
- Model registry + A/B traffic splitting.
- Schema migration `tx_canonical.v2+`.

---

## 📜 Giấy phép

Xem [`LICENSE`](LICENSE).
