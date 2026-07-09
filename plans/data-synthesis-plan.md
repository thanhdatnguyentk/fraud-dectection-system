# Kế hoạch tổng hợp & chuẩn bị dữ liệu cho hệ thống FDS
# Data Acquisition, Engineering & Synthesis Plan

> **Phiên bản:** 1.1 (Local / Personal Edition)
> **Ngày:** 2026-07-09  
> **Mục tiêu:** Chuẩn bị đầy đủ dữ liệu cho toàn bộ pipeline FDS (bản tinh gọn cho PC cá nhân 16GB RAM) đã được thiết kế tại `plans/fraud-detection-system-design.md`.  
> **Phương châm:** *Không train model trên CSV tĩnh — đẩy dữ liệu qua chính pipeline Redpanda → Faust/Quix → Redis → ONNX để đo lường thực tế.*

---

## Mục lục
1. [Tổng quan chiến lược](#1-tổng-quan-chiến-lược)
2. [Phân tích từng nguồn dữ liệu](#2-phân-tích-từng-nguồn-dữ-liệu)
3. [Kiến trúc dữ liệu tổng hợp](#3-kiến-trúc-dữ-liệu-tổng-hợp)
4. [Pipeline thu thập & chuẩn hóa](#4-pipeline-thu-thập--chuẩn-hóa)
5. [Data Generator cho streaming test](#5-data-generator-cho-streaming-test)
6. [Làm giàu dữ liệu (Feature Engineering off-line)](#6-làm-giàu-dữ-liệu-feature-engineering-off-line)
7. [Chiến lược tách train / eval / replay](#7-chiến-lược-tách-train--eval--replay)
8. [Lưu trữ & vận hành](#8-lưu-trữ--vận-hành)
9. [Lộ trình triển khai theo tuần](#9-lộ-trình-triển-khai-theo-tuần)
10. [Công cụ, thư viện & tiêu chí nghiệm thu](#10-công-cụ-thư-viện--tiêu-chí-nghiệm-thu)
11. [Phụ lục: Schema, glossary, rủi ro](#11-phụ-lục)

---

## 1. Tổng quan chiến lược

### 1.1 Bốn nguồn dữ liệu, bốn mục đích

| # | Dataset | Mục đích chính trong hệ thống | Sử dụng cho |
|---|--------|--------------------------------|-----------|
| 1 | **IEEE-CIS Fraud Detection** (Vesta) | Dữ liệu **chính** cho train ML tabular + test pipeline end-to-end | Feature engineering, XGBoost/LightGBM, calibration, replay |
| 2 | **ULB Credit Card Fraud** (Kaggle) | **POC nhanh** cho khối AI, sanity-check model | Validate ONNX, smoke-test Triton, demo nhẹ |
| 3 | **Sparkov Synthetic** | **Stress & load test** cho streaming | Kafka producer, Flink window, latency benchmark |
| 4 | **PaySim** | **GNN training** truy vết đường dây rửa tiền | Money-laundering graph, GNN node classification |

### 1.2 Nguyên tắc vàng

1. **Canonical Schema First** — mọi dataset được map về **một schema chuẩn `tx_canonical.v1`** trước khi đưa vào hệ thống.
2. **Stateless Ingest → Stateful Compute → Online Store** — không để CSV ở bất kỳ tầng nào trong production code.
3. **Replayability** — mọi giao dịch test phải có thể phát lại đúng thứ tự thời gian.
4. **No PII Leak** — tokenize/hash tất cả trường nhạy cảm ngay ở ingestion.
5. **Deterministic Split** — `user_id`/`device_fp` được dùng để split train/eval để tránh leakage.
6. **Right tool, right stage** — DuckDB/Polars cho off-line (Local PC), Redpanda/Faust cho streaming.

### 1.3 Pipeline 7 tầng (bird's-eye)

```
┌──────────────────────────────────────────────────────────────────────┐
│  (Raw) IEEE-CIS | ULB | Sparkov | PaySim                              │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼  scripts/ingest/*
┌──────────────────────────────────────────────────────────────────────┐
│  Tầng 1 — Extract & Validate   (PyArrow, Great Expectations)          │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Tầng 2 — Canonicalize         → tx_canonical.v1 (Parquet local)      │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Tầng 3 — Enrichment           (offline features, DuckDB/Polars)      │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Tầng 4 — Storage               Local Parquet + Postgres (control)    │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Tầng 5 — Train/Backtest        Polars + MLflow → ONNX               │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Tầng 6 — Online Mirror        Parquet → Redis (backfill)            │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Tầng 7 — Replay / Load Test   scripts/synth/ → Redpanda topic        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Phân tích từng nguồn dữ liệu

### 2.1 IEEE-CIS Fraud Detection (dataset chính — ✅ khuyến nghị dùng)

**Nguồn gốc:** Vesta Corporation, Kaggle competition (2019).  
**Quy mô:** ~590 k giao dịch ecommerce, ~118 feature sau merge; label `isFraud`.

**Cấu trúc gốc**

```
train_transaction.csv      (590 540 rows × 394 cols)
train_identity.csv        (144 227 rows × 41 cols)
  └─ TransactionID (key) ↔ Identity
```

**Cột quan trọng (group)**

| Nhóm | Ví dụ cột | Ý nghĩa |
|------|-----------|---------|
| Core | `TransactionDT`, `TransactionAmt`, `ProductCD`, `card1..card6` | Time (seconds since ref), amount, brand, payment card meta |
| Address | `addr1`, `addr2`, `dist1`, `dist2`, `P_emaildomain`, `R_emaildomain` | Billing/reshipping geo, distance |
| Identity | `DeviceInfo`, `id_30` (OS), `id_31` (browser), `id_33` (screen) | Device fingerprint |
| Network | `id_12..id_38` | TLS, language, proxy, match status |
| Vesta (V* 339 cột) | V1..V339 | Đặc trưng rule-engine đã ẩn danh, rất giàu tín hiệu |
| Label | `isFraud`, `isFraud (test only)` | Binary target |

**Điểm mạnh**

- **Khớp schema `ScoreRequest`** đã thiết kế: có `device_fp`, `ip`, `email_domain`, `country`, `mcc`.
- **V-features** chính là "rule engine output ẩn danh" → map thẳng sang quyết định soft-rule khi serve.
- Đủ lớn để train XGBoost mà vẫn dùng được GPU/LightGBM.

**Hạn chế & cách xử lý**

- Memory ~1.5 GB; dùng **Polars** thay Pandas để load nhanh gấp 5-10×.
- Missing ratio cao (40–60 % ở một số cột); dùng **native categorical** của LightGBM.
- `TransactionDT` không phải timestamp thật; phải map qua epoch giả định (giữ ordering).

### 2.2 ULB Credit Card Fraud (POC nhanh)

**Nguồn gốc:** Worldline + ULB ML, Kaggle (2016).  
**Quy mô:** 284 807 tx, 492 fraud (0.172 %). Features V1..V28 đã qua PCA; chỉ giữ `Time` & `Amount`.

**Vai trò**

- **Smoke test cho inference**: đảm bảo ONNX → Triton pipeline hoạt động mà không cần feature engineering.
- **POC UI / Demo** cho stakeholder.
- **Sanity benchmark** model nhỏ (logistic, XGBoost) trong môi trường nhẹ.

**Hạn chế**

- Không thể map sang `device_fp`, `geo`, `velocity` → **KHÔNG dùng cho chứng minh khả năng của feature store thời gian thực**.

### 2.3 Sparkov Synthetic (kiểm thử streaming)

**Nguồn gốc:** Brandon Harris (GitHub).  
**Quy mô:** ~1.3 M giao dịch giả lập theo timestamp thực; có demographics + merchant + MCC + lat/lon.

**Vai trò**

- **Load test streaming**: viết producer script phát 100-500 msg/giây vào `fds.tx.raw.v1`.
- **Test Faust/Quix windows**: kiểm tra velocity, distinct_country, impossible-travel (Local Streaming).
- **Test idempotency**: replay nhiều lần, đảm bảo không double-decision.

**Hạn chế**

- Không có label gian lận tự nhiên → dùng để **test infra**, không để train model chính.
- Có thể **inject nhãn giả** (rule-based) để benchmark AUC <-> cost.

### 2.4 PaySim (GNN training)

**Nguồn gốc:** UEBA-Sim, Kaggle (2017).  
**Quy mô:** ~6.3 M bản ghi, 11 cột (`step`, `type`, `amount`, `nameOrig`, `nameDest`, `oldbalanceOrg`, `newbalanceOrg`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`).

**Vai trò**

- Xây **đồ thị tài khoản ↔ giao dịch** cho GNN (GraphSAGE / GAT).
- Phát hiện **mule account chain**: các nút trung gian trong network laundering.

**Hạn chế**

- Synthetic, không phản ánh đặc thù thẻ tín dụng → dùng riêng cho GNN module, không trộn với IEEE-CIS.

### 2.5 Ma trận quyết định: dataset → module

| Module của hệ thống | IEEE-CIS | ULB | Sparkov | PaySim |
|---------------------|:--------:|:---:|:-------:|:------:|
| Stream pipeline (Redpanda/Faust)| ✅ train | ⚪ | ✅ **chính** | ✅ partial |
| Feature store real-time | ✅ train + backfill | ⚪ | ✅ backfill | ⚪ |
| XGBoost / LightGBM | ✅ **chính** | ✅ POC | ⚪ | ⚪ |
| Autoencoder | ✅ **chính** | ⚪ | ⚪ | ⚪ |
| GNN | ⚪ | ⚪ | ⚪ | ✅ **chính** |
| Rules engine | ⚪ (V-features tham khảo) | ⚪ | ⚪ | ⚪ |
| Load test & replay | ✅ replay | ⚪ | ✅ **chính** | ✅ partial |

---

## 3. Kiến trúc dữ liệu tổng hợp

### 3.1 Canonical Schema — `tx_canonical.v1`

Mọi dataset được chuẩn hóa về schema này **trước khi** vào bất kỳ stage nào:

| Field | Type | Required | Mô tả | Map từ |
|-------|------|----------|-------|--------|
| `tx_id` | string (ULID) | ✅ | ID duy nhất sinh ở ingestion | tự sinh |
| `ts_ms` | int64 | ✅ | Epoch ms | IEEE-CIS `TransactionDT` map qua epoch offset |
| `user_id` | string | ✅ | Tokenized user | IEEE-CIS `card1` hashed, ULB `Time*index`, Sparkov `cc_num` hashed, PaySim `nameOrig` |
| `device_fp` | string? | ⚪ | Device fingerprint | IEEE-CIS `DeviceInfo`+`id_31` hashed |
| `ip_hash` | string? | ⚪ | IP băm | IEEE-CIS `id_33` proxy IP hint (không có IP thật) |
| `email_domain_hash` | string? | ⚪ | Email domain băm | IEEE-CIS `P_emaildomain` hashed |
| `card_bin` | string? | ⚪ | BIN | IEEE-CIS `card1..6` extract |
| `merchant_id` | string? | ⚪ | Merchant tokenized | Sparkov merchant name hashed |
| `mcc` | string? | ⚪ | MCC code | IEEE-CIS `ProductCD`, Sparkov MCC |
| `channel` | enum | ✅ | card_present / ecommerce / mobile / transfer | suy ra từ dataset |
| `amount_minor` | int64 | ✅ | Số tiền × 100 | IEEE-CIS `TransactionAmt *100` |
| `currency` | string | ✅ | ISO 4217 | mặc định "USD" |
| `country` | string? | ⚪ | ISO 3166-1 alpha-2 | suy ra từ addr/email |
| `lat`, `lon` | float? | ⚪ | Toạ độ giao dịch | Sparkov lat/lon |
| `ip_country` | string? | ⚪ | Quốc gia IP (giả) | suy ra |
| `attributes` | map<string,string> | ⚪ | Bag-of-extras | tất cả feature còn lại |
| `label` | int? | ⚪ | 0/1 nếu dataset có | `isFraud` |
| `dataset_source` | enum | ✅ | ieee_cis / ulb / sparkov / paysim | tự tag |
| `schema_version` | int | ✅ | = 1 | tự tag |

> **Lưu ý:** Trường nào `null` được phép; ingestion bổ sung giá trị mặc định an toàn (empty string, 0) để tránh bug downstream.

### 3.2 Storage layout

```
s3://fds-data/
├── raw/
│   ├── ieee-cis/yyyy/mm/dd/transaction.parquet
│   ├── ieee-cis/yyyy/mm/dd/identity.parquet
│   ├── ulb/creditcard.csv
│   ├── sparkov/fraudTrain.csv
│   └── paysim/PS_20174392719_Eng.csv
├── canonical/
│   └── tx_canonical.v1/date=YYYY-MM-DD/source=ieee_cis/*.parquet
└── features/
    ├── offline/fact_transaction/
    ├── model_artifacts/xgb_v12/model.onnx
    └── model_artifacts/gnn_v3/model.pt
```

**Parquet partitioning (Local)**

```
Thư mục: source=ieee_cis/country=VN/
```

### 3.3 Quy tắc làm sạch & chuẩn hoá

1. **Loại bỏ trùng lặp** theo `tx_id` (sinh nếu thiếu).
2. **Tokenization/Hashing**: `bcrypt`(cost=10) cho user_id; **HMAC-SHA256** với secret trong Vault cho email_domain, IP, device_fp.
3. **Outlier capping**: `amount_minor` clip ở p99.9 để tránh skew.
4. **Time alignment**: chuyển `TransactionDT` của IEEE-CIS về epoch ms dựa trên offset cố định.
5. **Categorical cardinality**: giảm cardinality `card1..6`, `addr1..2` bằng WOE (Weight of Evidence) hoặc target-encoding.

### 3.4 Data Quality với Great Expectations

| Expectation | Áp dụng cho |
|-------------|-------------|
| `expect_column_values_to_not_be_null("tx_id")` | tất cả |
| `expect_column_values_to_match_regex("user_id", "^[a-f0-9]{32}$")` | tất cả (đã hash) |
| `expect_column_values_to_be_between("amount_minor", 0, 1_000_000_000)` | tất cả |
| `expect_table_row_count_to_be_between(200000, 1000000)` | từng dataset |
| `expect_column_proportion_of_unique_values_to_be_between("user_id", 0.05, 0.95)` | kiểm tra không quá unique |

---

## 4. Pipeline thu thập & chuẩn hóa

### 4.1 Cấu trúc thư mục

```
fraud-dectection-system/
├── data/
│   ├── raw/                           # gitignore
│   ├── canonical/                     # gitignore
│   └── artifacts/                     # gitignore
├── scripts/
│   ├── ingest/
│   │   ├── download_datasets.py
│   │   ├── ieee_cis_to_canonical.py
│   │   ├── ulb_to_canonical.py
│   │   ├── sparkov_to_canonical.py
│   │   └── paysim_to_canonical.py
│   ├── synth/
│   │   ├── kafka_producer.py
│   │   ├── scenario_runner.py
│   │   └── replay_log.py
│   ├── feature/
│   │   ├── build_offline_features.py
│   │   └── backfill_redis.py
│   └── train/
│       ├── train_xgb.py
│       ├── train_autoencoder.py
│       └── train_gnn.py
├── pipelines/                         # Airflow/Dagster DAGs
│   ├── ingest_dag.py
│   ├── train_dag.py
│   └── replay_dag.py
├── tests/
│   ├── test_canonical_schema.py
│   └── test_data_quality.py
└── plans/
    ├── fraud-detection-system-design.md
    └── data-synthesis-plan.md
```

### 4.2 Script mẫu — `ieee_cis_to_canonical.py`

```python
import polars as pl
import hashlib, hmac, os, uuid
from datetime import datetime, timezone

SECRET = bytes.fromhex(os.environ["PII_HMAC_KEY"])

def h(value: str) -> str:
    if value is None: return ""
    return hmac.new(SECRET, value.encode(), hashlib.sha256).hexdigest()

def to_canonical(df: pl.DataFrame) -> pl.DataFrame:
    offset_epoch = datetime(2017, 12, 1, tzinfo=timezone.utc).timestamp()
    return df.select([
        pl.lit(None).alias("tx_id").map_elements(lambda _: str(uuid.uuid4())),
        ((pl.col("TransactionDT") + offset_epoch) * 1000).cast(pl.Int64).alias("ts_ms"),
        h("card1").alias("user_id"),
        h(pl.col("DeviceInfo").fill_null("") + pl.col("id_31").fill_null("")).alias("device_fp"),
        h(pl.col("P_emaildomain")).alias("email_domain_hash"),
        pl.col("card1").cast(pl.Utf8).alias("card_bin"),
        pl.lit("vesta").alias("merchant_id"),
        pl.col("ProductCD").alias("mcc"),
        pl.lit("ecommerce").alias("channel"),
        (pl.col("TransactionAmt") * 100).cast(pl.Int64).alias("amount_minor"),
        pl.lit("USD").alias("currency"),
        pl.col("addr1").alias("country"),
        pl.lit(None).cast(pl.Float64).alias("lat"),
        pl.lit(None).cast(pl.Float64).alias("lon"),
        pl.col("isFraud").cast(pl.Int8).alias("label"),
        pl.lit("ieee_cis").alias("dataset_source"),
        pl.lit(1).alias("schema_version"),
    ])
```

### 4.3 DAG ingest (Airflow/Dagster)

```
download_datasets (manual/Bash script)
    └─> ieee_cis_to_canonical (Polars script)
    └─> ulb_to_canonical
    └─> sparkov_to_canonical
    └─> paysim_to_canonical
            └─> great_expectations_validate
                    └─> save_to_local_parquet
                            └─> emit_quality_metrics
```

- Schedule: chạy 1 lần cho batch IEEE-CIS/ULB/Sparkov/PaySim, sau đó trigger **cho dữ liệu mới**.
- Retry: 3 lần, exponential backoff.

---

## 5. Data Generator cho streaming test

### 5.1 Vai trò

Biến dataset tĩnh thành **luồng sự kiện thời gian thực** để kiểm thử toàn bộ pipeline:

```
                ┌────────────────────────────┐
Canonical ───►  │  scripts/synth/             │
Parquet         │  kafka_producer.py          │ ───► Redpanda topic fds.tx.raw.v1
                │  - load từng partition      │        (đúng schema JSON)
                │  - sort by ts_ms            │
                │  - phát theo rate điều chỉnh│
                └────────────────────────────┘
```

### 5.2 Tính năng bắt buộc

| Tính năng | Mô tả |
|-----------|--------|
| **Time-aware emit** | Phát sự kiện đúng khoảng cách thời gian (có thể tăng tốc ×N) |
| **Backpressure aware** | Đo `kafka.producer.records.await` để tránh nghẽn producer |
| **Scenario injection** | Chèn kịch bản (velocity attack, impossible travel, fraud ring) |
| **Idempotent replay** | Dùng `tx_id` cố định nên replay được nhiều lần |
| **Load profile** | ramp 100 → 1 000 → 10 000 → 50 000 TPS |
| **Metrics export** | Push Prometheus metrics: tx_emitted_total, emit_lag_ms |

### 5.3 Script mẫu — `kafka_producer.py` (rút gọn)

```python
import asyncio, json, os, time
from aiokafka import AIOKafkaProducer
import polars as pl

BROKER = os.getenv("KAFKA", "localhost:9092")
TOPIC = os.getenv("TOPIC", "fds.tx.raw.v1")
RATE = int(os.getenv("RATE_TPS", "5000"))
SPEEDUP = int(os.getenv("SPEEDUP", "60"))   # 1 giây data → SPEEDUP giây thực

async def produce(parquet_path: str):
    df = pl.read_parquet(parquet_path).sort("ts_ms")
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, linger_ms=2, acks="all")
    await producer.start()
    sent = 0
    prev_ts = df["ts_ms"][0]
    try:
        for row in df.iter_rows(named=True):
            now_target = (row["ts_ms"] - df["ts_ms"][0]) / 1000 / SPEEDUP
            time.sleep(max(0, now_target - (time.time() - start)))
            payload = json.dumps(row).encode()
            await producer.send_and_wait(TOPIC, payload, key=row["tx_id"].encode())
            sent += 1
    finally:
        await producer.stop()
```

### 5.4 Scenarios tự động

| Scenario | Mô tả | Mục đích test |
|----------|-------|---------------|
| `velocity_attack` | 30 tx trong 60 s cùng card | Hard rule + Redis counter |
| `impossible_travel` | NY → Tokyo trong 5 phút | Flink window + geo-distance |
| `device_spray` | 1 device, 50 user khác nhau | GNN/clustering |
| `mule_chain` | A → B → C → D trong 10 phút | PaySim replay cho GNN |
| `fat_finger` | amount gấp 10× lần trước của user | XGBoost feature |
| `holiday_spike` | burst tăng 3× traffic giờ cao điểm | HPA autoscaling |

### 5.5 Lệnh chạy tham khảo

```bash
# 1. Smoke test
python scripts/synth/kafka_producer.py \
    --parquet data/canonical/date=2026-06-01/source=ieee_cis/*.parquet \
    --rate 100 --speedup 600 --topic fds.tx.raw.v1

# 2. Load test
python scripts/synth/kafka_producer.py \
    --parquet data/canonical/source=sparkov/*.parquet \
    --rate 10000 --speedup 30 --topic fds.tx.raw.v1 --duration 600

# 3. Scenario replay
python scripts/synth/scenario_runner.py \
    --scenario velocity_attack --users 1000 --duration 600
```

---

## 6. Làm giàu dữ liệu (Feature Engineering off-line)

### 6.1 Mục tiêu

Tạo **offline feature set** để:
- Train XGBoost/LightGBM.
- Tính toán các backfill feature cho Redis khi user mới (chưa có streaming data).

### 6.2 Feature categories (gắn với dataset)

| Category | Ví dụ feature | Tính từ dataset |
|----------|---------------|------------------|
| **Velocity (offline)** | `tx_count_10m`, `amt_sum_1h` | DuckDB / Polars window function trên Parquet |
| **Distance** | `distance_from_last_km` | lat/lon Haversine (Sparkov) |
| **Identity match** | `id_01..38` gốc | IEEE-CIS (giữ nguyên) |
| **Behavioral** | `avg_amount_30d`, `pct_weekend` | groupby user rolling 30 ngày |
| **Merchant risk** | `fraud_rate_per_mcc` | groupby merchant |
| **Graph (PaySim)** | `degree_in`, `degree_out`, `pagerank` | NetworkX / cuGraph |

### 6.3 DuckDB/Polars script — `build_offline_features.py`

```python
import polars as pl

# Sử dụng Polars thay vì Spark để chạy nhẹ nhàng trên máy cá nhân
tx = pl.read_parquet("data/canonical/**/*.parquet")

features = tx.sort("ts_ms").with_columns([
    pl.col("tx_id").rolling_count(window_size="10m", by="ts_ms").over("user_id").alias("tx_count_10m"),
    (pl.col("amount_minor").rolling_sum(window_size="1h", by="ts_ms").over("user_id") / 100).alias("amt_sum_1h"),
    pl.col("country").rolling_nunique(window_size="24h", by="ts_ms").over("user_id").alias("distinct_country_24h")
])

features.write_parquet("data/features/offline/fact_features_offline.parquet")
```

### 6.4 Backfill Redis

Sau khi có `fact_features_offline`, script **đẩy snapshot cuối ngày** vào Redis (Single node) để warm cache cho ngày hôm sau:

```python
# Backfill: tối đa 1 user = 1 record, TTL = 30 ngày
df = pl.read_parquet("data/features/offline/fact_features_offline.parquet") \
       .group_by("user_id").last()
write_to_redis(df) # Sử dụng redis.asyncio pipeline để đẩy dữ liệu
```

---

## 7. Chiến lược tách train / eval / replay

### 7.1 Nguyên tắc split

- **Group-aware split**: chia theo `user_id` (group) để tránh leakage.
- **Time-based split** cho đánh giá temporal drift:

```
Train:  2017-11-01 → 2017-12-15   (70%)
Valid:  2017-12-16 → 2017-12-22   (15%)
Test:   2017-12-23 → 2017-12-31   (15%)
```

### 7.2 Negative sampling

- IEEE-CIS: ~3.5 % fraud → giữ nguyên phân phối, dùng `scale_pos_weight`.
- ULB: 0.172 % → bắt buộc SMOTE hoặc undersampling; SMOTE Tomek Links cho tabular.
- PaySim: 0.13 % fraud, có thêm `isFlaggedFraud`.

### 7.3 Replay test

- Sau khi train, **replay dataset qua Redpanda** với model mới ở chế độ shadow.
- So sánh p99 latency & score distribution với champion.

---

## 8. Lưu trữ & vận hành

### 8.1 Storage decision matrix

| Loại dữ liệu | Tầng | Retention | Encryption |
|--------------|------|-----------|------------|
| Raw CSV | Local Disk `raw/` | forever | Bỏ qua (Local) |
| Canonical Parquet | Local Disk `canonical/` | 2 năm | Bỏ qua (Local) |
| Parquet facts | Local Disk `features/` | 7 năm | Bỏ qua (Local) |
| Model artifacts | Local Disk `artifacts/` | indefinite | None |
| Redis features | single node | 30 ngày TTL | None |
| Decision audit | PostgreSQL | 7 năm | TDE |

### 8.2 Quyền truy cập (IAM tối thiểu)

| Role | Read | Write |
|------|------|-------|
| Data engineer | raw/*, canonical/* | canonical/* |
| ML engineer | canonical/*, features/*, model_artifacts/* | model_artifacts/* |
| Application (online) | Redis, decision_audit | Redis |
| Compliance auditor | decision_audit (PII tokenized) | — |

### 8.3 Bảo mật dữ liệu

- **PII tokenization** ngay ở ingestion (HMAC-SHA256, secret xoay vòng 90 ngày).
- **Masking** `lat/lon` xuống 3 chữ số thập phân (~110 m).
- **K-anonymity** ≥ 50 nếu export tổng hợp cho BI.

---

## 9. Lộ trình triển khai theo tuần

| Tuần | Việc cần làm | Tiêu chí nghiệm thu |
|------|--------------|---------------------|
| **W1** | Tải 4 dataset về `data/raw/`; thiết lập Polars + PyArrow môi trường | 4 file CSV lưu trữ local, hash recorded |
| **W2** | Viết 4 script `*_to_canonical.py`; sinh `data/canonical/` partition parquet | Đầu ra pass Great Expectations schema |
| **W3** | Iceberg init trên MinIO; load canonical; kiểm tra time-travel | `SELECT * FROM iceberg.db.fact_transaction VERSION AS OF ...` trả kết quả |
| **W4** | `build_offline_features.py` với 30 features; backfill Redis L1 | `redis-cli MGET feat:user:<id>:tx_count_10m` trả về số |
| **W5** | Train XGBoost baseline trên IEEE-CIS; convert ONNX | AUC ≥ 0.93, ONNX infer p99 < 5 ms local |
| **W6** | `kafka_producer.py` smoke test 100 TPS; verify Kafka topic + schema registry | Console consumer đọc được payload đúng schema |
| **W7** | Load test ramp 100 → 10 000 TPS với Sparkov; đo Flink lag | Lag < 1 s sustained ở 10 k TPS |
| **W8** | Train Autoencoder + GNN (PaySim → graph → cuGraph); blender | Ensemble AUC ≥ 0.96 trên test |
| **W9** | End-to-end replay: dataset ↦ Kafka ↦ Flink ↦ Redis ↦ Triton ↦ decision | Decision audit log khớp replay DSL output |
| **W10** | Scenario injection: velocity/impossible-travel/mule chain | Từng scenario được detect đúng quy tắc tương ứng |
| **W11** | Shadow A/B model mới vs champion; calibration, drift report | Champion vs Challenger dashboard hiển thị metric |
| **W12** | Documentation, handoff, runbook | README + onboarding mới chạy được toàn bộ từ zero |

---

## 10. Công cụ, thư viện & tiêu chí nghiệm thu

### 10.1 Stack khuyến nghị

| Lớp | Công cụ |
|-----|---------|
| Download | `kaggle api` |
| Processing | **Polars** (in-memory, tối ưu RAM), **DuckDB** |
| Schema validation | **Great Expectations** |
| Storage | **Local Parquet files** |
| Orchestration | Script đơn giản chạy bằng Bash hoặc Make |
| Streaming | **Redpanda** (Kafka-compatible), **Faust** hoặc **Quix Streams** |
| ML training | **XGBoost**, **LightGBM**, **PyTorch (Autoencoder)**, **PyTorch Geometric / DGL (GNN)** |
| Model export | **ONNX**, **skl2onnx**, **onnx2torch** |
| Quality tests | **pytest**, **pandera** |
| CLI/UI trực quan | **Streamlit** (EDA), **MLflow** (tracking) |

### 10.2 Tiêu chí nghiệm thu chất lượng dữ liệu

| Metric | Target |
|--------|--------|
| Row loss canonical | < 0.1 % |
| Cardinality `user_id` / fraud ratio | khớp dataset gốc ±0.5 % |
| Null ratio sau FE | < 5 % cho feature chính |
| Schema evolution | forward-compatible trong cùng `schema_version` |

---

## 11. Phụ lục

### 11.1 Pseudonymization cheatsheet

| Trường | Phương pháp |
|--------|-------------|
| `card1..6`, `P_emaildomain` | HMAC-SHA256 |
| `nameOrig`, `nameDest` (PaySim) | HMAC-SHA256 |
| `device_fp` | SHA256(device_info + ua + salt) |
| `lat`, `lon` | làm tròn 3 chữ số |
| `TransactionDT` | offset tới epoch cố định |
| `ip` (nếu có) | 1-way hash + 8-bit prefix network giữ |

### 11.2 Glossary

- **PII** — Personally Identifiable Information.
- **HMAC** — Hash-based Message Authentication Code.
- **ONNX** — Open Neural Network Exchange.
- **POC** — Proof of Concept.
- **GNN** — Graph Neural Network.
- **TTL** — Time To Live (Redis).
- **Backfill** — lấp đầy quá khứ từ batch vào store online.
- **Shadow A/B** — chạy song song 2 model mà không ảnh hưởng decision thật.

### 11.3 Ma trận rủi ro & giảm thiểu

| Rủi ro | Xác suất | Tác động | Giảm thiểu |
|--------|---------|---------|-----------|
| Lộ PII từ raw CSV | Trung bình | Cao | Không commit CSV; mount bucket chỉ-đọc; encryption KMS. |
| Dataset quá lớn gây OOM | Cao | Trung bình | Polars/streaming; partition theo date; không load full vào Pandas. |
| Drift khi replay | Trung bình | Trung bình | Sinh nhãn rule-based giả lập fraud để benchmark. |
| Mất timestamp ordering | Thấp | Cao | Sort toàn cục theo `ts_ms` trước khi produce; idempotent key. |
| Data leakage qua user_id | Trung bình | Cao | Group split + verify leakage detection (nunique user). |
| Overfit do feature V339 | Cao | Trung bình | Regularization mạnh; early stopping; SHAP pruning. |

### 11.4 Tham chiếu

- IEEE-CIS Fraud Detection — Kaggle (2019).
- ULB Credit Card Fraud — Kaggle (Worldline + ULB ML, 2016).
- Sparkov GitHub — Brandon Harris.
- PaySim — UEBA-Sim (2017).
- Apache Iceberg spec & docs.
- NVIDIA Triton Inference Server Best Practices.

---

**Kết luận:** Với bốn nguồn dữ liệu (IEEE-CIS chính, ULB POC, Sparkov streaming, PaySim GNN), chiến lược 7 tầng ở mục 3 và data generator ở mục 5 cho phép toàn bộ hệ thống FDS được **train trên batch, test trên stream, replay được khi cần** — vừa đáp ứng yêu cầu kỹ thuật (< 50 ms p99), vừa chứng minh được bằng số liệu thực tế thay vì notebook tĩnh.
