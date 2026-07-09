# Kế hoạch thiết kế hệ thống phát hiện gian lận thời gian thực
# Real-Time Fraud Detection System — Architecture & Implementation Plan

> **Phiên bản:** 1.1 (Local / Personal Edition)
> **Ngày:** 2026-07-09  
> **Phạm vi:** Thiết kế tinh gọn chạy trên PC cá nhân (16GB RAM, GPU GTX 3050)
> **Ràng buộc cứng:** `T_total < 50 ms` (p99)  
> **Mục tiêu:** ~ 100 - 500 TPS (Local Stress Test), FPR ≤ 1 %, recall fraud ≥ 95 %

---

## Mục lục
1. [Tổng quan bài toán và mục tiêu](#1-tổng-quan-bài-toán-và-mục-tiêu)
2. [Yêu cầu chức năng & phi chức năng](#2-yêu-cầu-chức-năng--phi-chức-năng)
3. [Kiến trúc tổng thể](#3-kiến-trúc-tổng-thể)
4. [Thiết kế chi tiết các phân hệ](#4-thiết-kế-chi-tiết-các-phân-hệ)
5. [Thiết kế cơ sở dữ liệu](#5-thiết-kế-cơ-sở-dữ-liệu)
6. [Thiết kế API](#6-thiết-kế-api)
7. [Mô hình AI & Feature Engineering](#7-mô-hình-ai--feature-engineering)
8. [Bảo mật & Tuân thủ](#8-bảo-mật--tuân-thủ)
9. [Observability & SRE](#9-observability--sre)
10. [Triển khai & Lộ trình](#10-triển-khai--lộ-trình)
11. [Phụ lục: Định nghĩa, thuật ngữ, tham chiếu](#11-phụ-lục)

---

## 1. Tổng quan bài toán và mục tiêu

### 1.1 Bài toán nghiệp vụ
Hệ thống phát hiện gian lận (Fraud Detection System — FDS) phải đánh giá **mọi giao dịch** trong thời gian thực trước khi acquirer/issuer phản hồi merchant, đảm bảo:

- **APPROVE** giao dịch hợp lệ → giữ trải nghiệm người dùng mượt mà.
- **DECLINE** giao dịch gian lận → giảm tổn thất (chargeback, refund, reputation).
- **CHALLENGE** (OTP, 3DS, biometric) → đánh đổi FPR/recall hợp lý.

### 1.2 "Lời nguyền" 50 ms

```
T_total = T_ingress + T_feature_fetch + T_inference + T_decision + T_egress
        < 50 ms (p99)
```

| Bước                       | Budget (ms) | Ghi chú                                    |
| -------------------------- | ----------- | ------------------------------------------ |
| Network/edge ingress       | 1           | gRPC/protobuf, mTLS-terminating LB         |
| Feature fetch (Redis/DDB)  | 3–5         | pipelined MGET + local cache               |
| Stream-feature recompute   | 2–4         | Flink worker co-located                    |
| Ensemble inference (3 mô hình) | 5–8        | NVIDIA Triton, ONNX-runtime                |
| Rules engine + business logic | 1–3      | DSL compile → native code                  |
| Decision + response        | 1           | HTTP/2 push                               |
| Head-room / GC / jitter    | 5–10        | budget an toàn, GC tuning, Pinning         |

### 1.3 Mục tiêu kinh doanh
- **Giảm chargeback loss** 40–60 % trong 12 tháng.
- **FPR ≤ 1 %** (false positive rate) trên tổng giao dịch hợp lệ.
- **Recall fraud ≥ 95 %** trên tập test giữ chứng (holdout).
- **Throughput** Đạt 100 - 500 TPS ổn định trên môi trường Local (16GB RAM).

---

## 2. Yêu cầu chức năng & phi chức năng

### 2.1 Yêu cầu chức năng (Functional)
| ID    | Mô tả                                                                                                 |
| ----- | ------------------------------------------------------------------------------------------------------ |
| FR-01 | Tiếp nhận giao dịch qua REST/gRPC/event-stream, trả về quyết định trong < 50 ms (p99).                  |
| FR-02 | Tính toán > 200 feature từ dữ liệu lịch sử + dữ liệu luồng.                                            |
| FR-03 | Chạy ensemble ≥ 2 mô hình (XGBoost + Autoencoder hoặc GNN), trả về risk-score ∈ [0,1].                  |
| FR-04 | Áp dụng hard rules (danh sách đen, BIN cấm, velocity cứng) trước/sau ML.                              |
| FR-05 | Hỗ trợ chiến lược kết hợp (combine strategy): score → action (APPROVE/CHALLENGE/DECLINE/REVIEW).        |
| FR-06 | Lưu vết mọi quyết định (audit log) phục vụ tra soát và huấn luyện lại (labeling).                       |
| FR-07 | Cung cấp API quản trị: CRUD rules, blacklist, threshold, model promotion, A/B traffic.                  |
| FR-08 | Webhook callback khi quyết định là CHALLENGE/DECLINE để upstream retry/notify.                          |
| FR-09 | Hỗ trợ manual override & case management cho reviewer.                                                 |
| FR-10 | Streaming feature recompute (ví dụ tx-count trong 10 phút) qua Flink/Kafka Streams.                     |

### 2.2 Yêu cầu phi chức năng (Non-functional)
| Nhóm             | Yêu cầu                                                                                                              |
| ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| Hiệu năng        | p99 ≤ 50 ms; tối ưu hóa bộ nhớ cho 16GB RAM và VRAM của GTX 3050.                                                     |
| Khả dụng          | Chạy Single-node trên Docker Compose.                                                                                 |
| Nhất quán         | Eventual consistency cho feature (Redis); strong consistency cho rules (Postgres).                                    |
| Quan sát được     | Metrics (Prometheus cục bộ) hoặc logging cơ bản ra file.                                                              |
| Bảo mật           | JWT cơ bản, không cần mTLS hay Vault cho môi trường Local.                                                           |
| Khả chuyển (portability) | Docker Compose `docker-compose.yml` chạy một lệnh lên toàn bộ stack.                                           |

---

## 3. Kiến trúc tổng thể

### 3.1 Sơ đồ kiến trúc (logical view)

```
                         ┌─────────────────────────────────────────────────────────────┐
                         │                  EDGE / INGESTION LAYER                    │
 Merchant / POS / App ───►│  WAF → API Gateway → gRPC/REST → Load Balancer (Envoy)  │
                         │                  rate-limit, mTLS, OAuth2                   │
                         └─────────────────────────────┬───────────────────────────────┘
                                                          │ transaction.requested (protobuf)
                                                          ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                       ORCHESTRATION & DECISION LAYER                              │
│  ┌────────────────┐    ┌──────────────────────┐    ┌─────────────────────────┐    │
│  │ Scoring Service │ ◄──┤   Rules Engine       │    │  Decision Orchestrator  │◄──┐│
│  │ (FastAPI/async) │    │ (DSL: simpleeval)    │    │ (strategy combinator)   │   ││
│  └────────┬───────┘    └────────────┬─────────┘    └────────────┬────────────┘   ││
│           │                         │                           │                ││
└───────────┼─────────────────────────┼───────────────────────────┼────────────────┘│
            │                         │                           │                  │
            ▼                         ▼                           ▼                  │
┌────────────────────┐   ┌──────────────────────┐    ┌──────────────────────────┐     │
│ INFERENCE LAYER    │   │  HYBRID: edge rules   │    │  AUDIT EVENT BUS (Kafka) │─────┘
│ ┌────────────────┐ │   │  + ML score combine   │    │  topic: fds.decision.v1  │
│ │ Triton Server  │ │   └──────────────────────┘    └──────────────────────────┘
│ │ ONNX runtime   │ │
│ │ Model A XGBoost│ │
│ │ Model B AutoEnc│ │
│ │ Model C GNN    │ │
│ └────────────────┘ │
└─────────┬──────────┘
          │ retrieve features (vector)
          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          FEATURE PLANE                                       │
│  ┌────────────────────┐    ┌─────────────────────────┐    ┌──────────────┐  │
│  │ Online Feature     │    │ Stream Processor        │    │ Offline      │  │
│  │ Store (Redis      │◄──┤ (Faust / Quix Streams)  │    │ Feature Store│  │
│  │  Single Node)     │    │ recompute rolling stats │    │ (DuckDB/Parquet│  │
│  └────────────────────┘    └─────────────────────────┘    │  + Parquet)  │  │
│                                                              └──────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
                                                                          │
                            ┌─────────────────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DATA PLANE                                            │
│  Redpanda (Single Node)      ·  DuckDB + Local Parquet ·  Postgres (Single)   │
│  Local Disk (models, logs)                                                     │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Sơ đồ luồng dữ liệu (request hot path)

```
Merchant ──gRPC──► Ingestion ──► Decision Orchestrator
                                       │
                ┌──────────────────────┼────────────────────────┐
                ▼                      ▼                        ▼
        Hard-Rules Eval         Feature Fetch           (Flink window) ──┐
                │              (Redis MGET)                            │
                ▼                      │                                │
        Short-circuit?                 ▼                                │
                │          Vector x ∈ R^N                              │
                │                      │                                │
                │                      ▼                                │
                │            Inference (Triton/ONNX)                  │
                │                      │                                │
                │       ┌──────────────┼──────────────┐                │
                │       ▼              ▼              ▼                │
                │   XGBoost       Autoencoder        GNN               │
                │       │              │              │                │
                │       └──────────────┬──────────────┘                │
                │                      ▼                                │
                │            Combine (weighted/blend)                  │
                ▼                      ▼                                │
        Soft-Rules Eval ◄──── Risk Score s ∈ [0,1]                      │
                                       │                                │
                                       ▼                                │
                          Decision (APPROVE/CHALLENGE/DECLINE/REVIEW) ──┘
                                       │
                                       ▼
                                Response (≤ 50 ms p99)
                                       │
                                       ▼
                       Kafka: fds.decision.v1 → Lakehouse / BI
```

### 3.3 Nguyên tắc kiến trúc
1. **Stateless scoring pods** — mọi trạng thái động nằm trong Kafka/Redis, không trong RAM process.
2. **Sidecar inference** — tách inference thành process riêng (Triton) để GPU/CPU được phân bổ đúng cách và OOM không kéo sập decision service.
3. **CQRS** — tách đường đọc (online) và đường ghi/audit (offline).
4. **Defensive degrade** — khi Redis chậm, fallback local cache (Caffeine, 5–10 ms refresh); khi model lỗi, chỉ dùng rules.
5. **Idempotency** — `tx_id` là khóa, tránh double-charge trong trường hợp retry.
6. **Back-pressure** — load shedding ưu tiên merchant tier 1; thông báo queue length qua header.

---

## 4. Thiết kế chi tiết các phân hệ

### 4.1 Stream Processing (Apache Flink + Kafka)

| Thành phần               | Vai trò                                                                                  |
| ------------------------ | ---------------------------------------------------------------------------------------- |
| **Message Broker**       | **Redpanda** (1 container): Rất nhẹ, tương thích 100% API Kafka, tiết kiệm RAM hơn Kafka gốc. |
| **Schema**               | Bỏ qua Schema Registry, dùng trực tiếp JSON (orjson) để giảm độ phức tạp hệ thống cục bộ. |
| **Stream Processor**     | Dùng **Faust** hoặc **Quix Streams** (Python) thay vì Flink. Giữ mọi thứ bằng Python để dễ debug và ít tốn RAM. |
| **State backend**        | Faust/Quix lưu state cục bộ qua RocksDB/SQLite. |
| **Watermark & window**   | Event-time, allowedLateness 5 s; window Tumbling 10 s / Sliding 1 m.                      |

**Topic naming convention**

```
fds.tx.raw.v1                 # giao dịch thô từ ingestion
fds.tx.enriched.v1            # sau khi join với profile lookup
fds.feature.stream.v1         # feature tính trên luồng
fds.decision.v1               # quyết định sau cùng
fds.audit.v1                  # audit có chữ ký
```

**Đảm bảo chất lượng**
- Exactly-once qua Kafka transactional producer + Flink two-phase commit.
- Deduplication key: `(merchant_id, terminal_id, stan, rrn)`.

### 4.2 Real-time Feature Store

| Layer          | Tech                          | TTL                  | Dùng cho                       |
| -------------- | ----------------------------- | -------------------- | ------------------------------ |
| L0 hot-cache   | `asyncache` (Python dict)     | 30 s                 | Giảm tải cho Redis cục bộ |
| L1 online      | Redis (1 container)           | 1–30 ngày (key TTL)  | MGET pipelined, lưu RAM  |
| L2 long-cache  | (Bỏ qua cho Local)            | -                    | Giảm tải RAM cho máy cá nhân |
| Offline store  | DuckDB đọc thư mục Parquet    | vĩnh viễn            | Train/eval/BI            |

**Cấu trúc key**

```
feat:user:{user_id}:tx_count_10m
feat:user:{user_id}:amt_sum_1h
feat:card:{card_bin}:reject_ratio_24h
feat:device:{device_fp}:distinct_users_24h
feat:ip:{ip}:risk_score
feat:merch:{merchant_id}:chargeback_rate_30d
feat:geo:last_location:{user_id}:ts         # cho impossible-travel
```

**Schema feature vector**

```json
{
  "entity_id": "user_abc123",
  "version": 12,
  "features": {
    "tx_count_10m": 4,
    "tx_count_1h": 11,
    "amt_sum_1h": 1850.50,
    "distinct_mcc_1h": 3,
    "distinct_country_24h": 1,
    "distance_from_last_km": 2.3,
    "seconds_since_last_tx": 47,
    "device_fp_seen_before": true,
    "ip_risk": 0.05,
    "merchant_chargeback_rate": 0.012,
    "card_velocity_bin": "high"
  },
  "as_of_ts": "2026-07-09T07:14:22.123Z"
}
```

**Update strategies**
- **Write-through** từ Flink jobs cho counters (tx_count_10m, amt_sum_1h).
- **Write-back** qua async event cho derived features (distance_from_last_km).
- **TTL-based eviction** + **lazy recompute** cho batch features hàng ngày.

**Tối ưu RAM (16GB)**
- Lưu log và feature cũ ra đĩa (Parquet), chỉ load key active vào Redis (giữ Redis < 2GB).
- Dùng Probabilistic data structures của Redis (HyperLogLog) để đếm số user/ip mà không tốn RAM.

### 4.3 Inference Engine (NVIDIA Triton + ONNX)

| Mô hình                | Loại              | Đầu vào                          | Đầu ra      | Latency budget |
| ---------------------- | ----------------- | -------------------------------- | ----------- | -------------- |
| `model_xgb_vN`         | XGBoost (tree)    | 220 numeric/categorical features | logit ∈ ℝ   | 1–2 ms         |
| `model_aae_vN`         | Autoencoder (DL)  | cùng vector                      | recon-error | 2–3 ms (GPU)   |
| `model_gnn_vN`         | GNN (mini graph)  | 2-hop subgraph xung quanh user  | score ∈ ℝ   | 3–5 ms (GPU)   |
| `meta_blender_vN`      | Logistic / NN     | 3 score trên + 12 context feature | p_fraud    | < 1 ms         |

**Pipeline optimization**
- Convert tất cả mô hình sang **ONNX**; compile sang **TensorRT** cho GPU.
- Batching động (dynamic batching) với `max_delay = 2 ms`.
- Model warm-pool; tránh cold-start lần đầu mỗi process.
- Triton ensemble mode: 1 request ⇒ 3 infer ⇒ 1 meta ⇒ response.

**Quản lý tài nguyên GPU/CPU (GTX 3050 - 4GB/8GB VRAM)**
- Chuyển tất cả mô hình sang **ONNX** với FP16 (nửa độ chính xác) để giảm nửa dung lượng VRAM.
- Có thể dùng `onnxruntime-gpu` nhúng trực tiếp vào FastAPI thay vì chạy Triton Server rời nếu Triton ăn quá nhiều RAM lúc khởi động.
- Batching động vẫn áp dụng, nhưng để batch size nhỏ (vd: max_batch=16) tránh tràn bộ nhớ GPU.

**Model governance**
- Mỗi model có `model_card.json` (provenance, dataset SHA, metrics, owner).
- Registry: MLflow / BentoML / internal; chỉ artifact đã sign mới được promote lên `prod`.
- Canary 5 % traffic trong 24 h, tự rollback nếu AUC giảm > 0.5 %.

### 4.4 Rules Engine & Decision Orchestration

**Kiến trúc 2 cấp**

```
┌─────────────────────────────┐
│   Hard-Rules (pre-ML)        │  • blacklist BIN, device, IP, email
│   • chạy < 1 ms              │  • velocity vượt ngưỡng cứng (10 phút > 20 tx)
│   • short-circuit DECLINE    │  • MCC cấm, country cấm, amount > limit
└─────────────────────────────┘
                │
                ▼
        ML inference (3 mô hình)
                │
                ▼
┌─────────────────────────────┐
│   Soft-Rules (post-ML)       │  • kết hợp score với policy
│   • DSL: Python simpleeval   │  • tiered action
└─────────────────────────────┘
                │
                ▼
        Decision Orchestrator
        ▸ APPROVE / CHALLENGE / DECLINE / REVIEW
```

**Rule DSL — ví dụ**

```
rule "high_amount_overnight" {
  when (
    amount > 1500.00
    AND hour >= 0 AND hour <= 5
    AND channel == "card_present"
  )
  then BOOST 0.15
  priority 100
}

rule "novel_device_high_amount" {
  when (
    model_xgb_score >= 0.55
    AND device_age_hours < 24
    AND amount > 500
  )
  then CHALLENGE otp_sms
}
```

Sử dụng `simpleeval` (thuần Python) để đánh giá trực tiếp trên RAM. Tốc độ đánh giá 1 rule phức tạp mất chưa tới 0.1 ms, loại bỏ hoàn toàn CGO/WASM overhead.

**Combining strategy — score → action**

```
function decide(score s, context c):
  if c.hard_denied: return DECLINE
  if c.step_up_required: return CHALLENGE(3ds)
  if s < 0.30: return APPROVE
  if s < 0.60: return APPROVE   # low-risk profile
  if s < 0.80: return CHALLENGE(otp)
  if s < 0.92: return CHALLENGE(biometric)
  else:           return DECLINE
```

Có thể cấu hình theo `policy_id` (vd: `policy_visa_domestic`, `policy_mastercard_cn`).

### 4.5 Audit & Replay

- Mỗi quyết định được publish sang `fds.decision.v1` với:
  - `tx_id`, `user_id`, `merchant_id`, `request_hash`.
  - `features_snapshot` (đã hash nhưng giữ mapping sang feature-store rev).
  - `model_versions`, `ruleset_version`, `policy_id`, `decision`, `latency_ms`.
- Lakehouse (Iceberg) partition theo `date/hour`, giữ 7 năm tuân thủ PCI/PSD2.
- Replay: re-run inference trên historical events với phiên bản model bất kỳ để back-test.

---

## 5. Thiết kế cơ sở dữ liệu

### 5.1 Lựa chọn công nghệ

| Mục đích                 | Công nghệ                              | Lý do                                                              |
| ------------------------ | -------------------------------------- | ------------------------------------------------------------------ |
| Event bus                | **Redpanda**                           | Nhẹ hơn Kafka, chỉ cần 1 Docker container, chạy cực mượt trên máy cá nhân. |
| Online KV feature store  | **Redis** (Single Node)                | Nhẹ, đáp ứng dư sức 500 TPS cục bộ.                                |
| Online transactional     | **PostgreSQL 16**                      | Quản lý rules, policies, audit nhẹ nhàng.                          |
| Analytics & Offline      | **DuckDB + Parquet local**             | Siêu tối ưu cho máy tính cá nhân. Xử lý hàng triệu dòng không cần RAM lớn. Thay thế hoàn toàn ClickHouse/Scylla/Iceberg. |

### 5.2 Schema PostgreSQL (control plane)

```sql
-- Rules & policies
CREATE TABLE rule_set (
  id           UUID PRIMARY KEY,
  name         TEXT NOT NULL,
  version      INT NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('draft','staging','prod','archived')),
  policy_id    UUID REFERENCES policy(id),
  wasm_bytes   BYTEA NOT NULL,
  created_by   TEXT NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT now(),
  UNIQUE (name, version)
);

CREATE TABLE policy (
  id           UUID PRIMARY KEY,
  name         TEXT NOT NULL,
  description  TEXT,
  thresholds   JSONB NOT NULL,    -- { approve:0.3, challenge:0.6, ... }
  decision_map JSONB NOT NULL,    -- map tier -> action
  created_at   TIMESTAMPTZ DEFAULT now()
);

-- Blacklists
CREATE TABLE blacklist_entry (
  id           BIGSERIAL PRIMARY KEY,
  kind         TEXT NOT NULL CHECK (kind IN ('card_bin','device_fp','ip','email','user','merchant')),
  value_hash   TEXT NOT NULL,            -- hash để không lộ PII
  raw_token    TEXT NOT NULL,            -- tokenized PII
  reason       TEXT,
  expires_at   TIMESTAMPTZ,
  created_by   TEXT NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_blacklist_kind_value ON blacklist_entry(kind, value_hash);

-- Model registry
CREATE TABLE model (
  id            UUID PRIMARY KEY,
  name          TEXT NOT NULL,           -- e.g. "xgb_v12"
  framework     TEXT NOT NULL,           -- onnx, tensorrt, pytorch
  artifact_uri  TEXT NOT NULL,           -- s3://.../model.onnx
  metrics       JSONB,                   -- {auc:0.97, recall_at_1fpr:0.92}
  status        TEXT NOT NULL,
  signed_hash   TEXT NOT NULL,           -- signature
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE model_promotion (
  id            BIGSERIAL PRIMARY KEY,
  model_id      UUID REFERENCES model(id),
  traffic_pct   INT NOT NULL,            -- 5, 25, 100
  started_at    TIMESTAMPTZ,
  ended_at      TIMESTAMPTZ,
  decided_by    TEXT NOT NULL,
  rollback_cause TEXT
);

-- Audit (lightweight; full audit goes to Kafka)
CREATE TABLE decision_audit (
  tx_id         TEXT PRIMARY KEY,
  user_id       TEXT NOT NULL,
  merchant_id   TEXT,
  decision      TEXT NOT NULL,
  score         NUMERIC(5,4) NOT NULL,
  rule_set_id   UUID,
  model_ver     TEXT,
  latency_ms    INT,
  decided_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_user_time ON decision_audit(user_id, decided_at DESC);
CREATE INDEX idx_audit_merchant_time ON decision_audit(merchant_id, decided_at DESC);
```

### 5.3 Schema Kafka event

**`fds.tx.raw.v1`** (Protobuf)

```protobuf
syntax = "proto3";
package fds.tx.v1;
message RawTx {
  string tx_id        = 1;
  string user_id      = 2;
  string card_bin     = 3;
  string device_fp    = 4;
  string merchant_id  = 5;
  string mcc          = 6;
  string channel      = 7;   // card_present | ecommerce | mobile
  int64  amount_minor = 8;   // cents
  string currency     = 9;
  string country      = 10;
  double lat          = 11;
  double lon          = 12;
  int64  ts_ms        = 13;
  map<string,string> extra = 14;
}
```

**`fds.decision.v1`** (JSON hoặc Avro)

```json
{
  "tx_id": "01J7...",
  "user_id": "u_8f...",
  "decision": "CHALLENGE",
  "challenge_kind": "otp_sms",
  "score": 0.74,
  "scores_by_model": { "xgb": 0.71, "aae": 0.66, "gnn": 0.79, "blender": 0.74 },
  "rules_fired": ["novel_device_high_amount", "high_amount_overnight"],
  "policy_id": "policy_visa_domestic_v3",
  "model_versions": { "xgb": "v12", "aae": "v4", "gnn": "v7" },
  "latency_ms": 27,
  "decided_at": "2026-07-09T07:14:22.171Z",
  "trace_id": "00-..."
}
```

### 5.4 Schema Iceberg (lakehouse)

```
iceberg.db.fact_transaction            # raw + enriched, partition (date, country)
iceberg.db.fact_decision               # decision events
iceberg.db.dim_user_profile            # SCD2 profile
iceberg.db.dim_merchant
duckdb_tables.fact_model_evaluation       # batch metrics per model
```

Sử dụng **DuckDB** để query trực tiếp trên các file `.parquet` lưu ở thư mục local `data/lake/`.

### 5.5 Partitioning & Retention

| Bảng/Topic         | Partition key         | Retention          |
| ------------------ | --------------------- | ------------------ |
| `fds.tx.raw.v1`    | `tx_id` murmur3       | 7 ngày hot + S3 1 năm |
| `fds.decision.v1`  | `tx_id`               | 30 ngày hot + 7 năm archive |
| `decision_audit`   | `decided_at` ngày     | 7 năm              |
| `blacklist_entry`  | hash                  | vĩnh viễn (có expires) |

---

## 6. Thiết kế API

### 6.1 Style & Conventions
- **Hot-path (scoring)**: REST + JSON cực nhanh (FastAPI + orjson + uvloop), mTLS.
- **Control-plane (admin)**: REST + JSON, OAuth2 Bearer JWT.
- **Webhook (outbound)**: HTTPS + HMAC signature.
- Pagination: cursor (`next_cursor`).
- Idempotency: header `Idempotency-Key`.
- API style: resource-oriented, RFC 7807 (problem+json) cho lỗi.
- Phiên bản: URL prefix `/v1/...`; header `Accept: application/vnd.fds.v2+json` cho fine-grained.

### 6.2 Authentication & Authorization
- **Ingress service-to-service**: mTLS với SPIFFE/SPIRE làm workload identity.
- **Admin API**: OAuth2/OIDC (Keycloak/Okta); scope `fds:rules:write`, `fds:rules:read`.
- **Webhook outbound**: chữ ký HMAC-SHA256; recipient whitelist & per-tenant secret.

### 6.3 API Endpoints

#### 6.3.1 Hot-path: `ScoreTransaction`

**Endpoint gRPC** `fds.scoring.v1.ScoringService/Score`

**Request (Protobuf)**

```protobuf
message ScoreRequest {
  string tx_id          = 1;
  string user_id        = 2;
  string card_bin       = 3;
  string device_fp      = 4;
  string merchant_id    = 5;
  string mcc            = 6;
  string channel        = 7;
  int64  amount_minor   = 8;
  string currency       = 9;
  string country        = 10;
  double lat            = 11;
  double lon            = 12;
  int64  ts_ms          = 13;
  map<string,string> attributes = 14;
  string policy_id      = 15;   // optional
}
```

**Response (Protobuf)**

```protobuf
message ScoreResponse {
  string tx_id          = 1;
  enum Decision { UNKNOWN=0; APPROVE=1; CHALLENGE=2; DECLINE=3; REVIEW=4; }
  Decision decision     = 2;
  enum ChallengeKind { NONE=0; OTP=1; THREE_DS=2; BIOMETRIC=3; PIN=4; }
  ChallengeKind challenge = 3;
  double score          = 4;          // p_fraud
  repeated string rules_fired = 5;
  string rule_set_version  = 6;
  string model_versions    = 7;       // JSON
  int32  latency_ms        = 8;
  string trace_id          = 9;
}
```

**HTTP/JSON mirror** (cho merchant dễ tích hợp):

```
POST /v1/score
Authorization: Bearer <jwt>
Content-Type: application/json
Idempotency-Key: 9b1c-...
```

**Request body**

```json
{
  "tx_id": "01J7F2A4QX9D2PE5K7HBN3M2RX",
  "user_id": "u_8f12c9",
  "card_bin": "448588",
  "device_fp": "fp_01HZP2...",
  "merchant_id": "m_amzn_us",
  "mcc": "5942",
  "channel": "ecommerce",
  "amount_minor": 12999,
  "currency": "USD",
  "country": "US",
  "lat": 37.7749,
  "lon": -122.4194,
  "ts_ms": 1720515262123,
  "attributes": { "ip": "203.0.113.45", "user_agent_hash": "..." },
  "policy_id": "policy_visa_domestic_v3"
}
```

**Response body (200 OK)**

```json
{
  "tx_id": "01J7F2A4QX9D2PE5K7HBN3M2RX",
  "decision": "CHALLENGE",
  "challenge": { "kind": "OTP", "channel": "sms", "ttl_seconds": 120 },
  "score": 0.74,
  "scores_by_model": { "xgb": 0.71, "aae": 0.66, "gnn": 0.79, "blender": 0.74 },
  "rules_fired": ["novel_device_high_amount", "high_amount_overnight"],
  "policy": "policy_visa_domestic_v3",
  "model_versions": { "xgb": "v12", "aae": "v4", "gnn": "v7", "blender": "v8" },
  "latency_ms": 27,
  "trace_id": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
}
```

**Error responses**

| Code | Khi nào                                                     | Body ví dụ (RFC7807)                                       |
| ---- | ----------------------------------------------------------- | ---------------------------------------------------------- |
| 400  | Validation (amount âm, currency sai)                        | `{ "type":"validation", "detail":"amount_minor must be >0" }` |
| 401  | Thiếu/không hợp lệ JWT/mTLS                                 | `{ "type":"auth", "detail":"invalid token" }`              |
| 403  | Không có scope                                              | `{ "type":"authz", "detail":"missing fds:score" }`         |
| 409  | Idempotency-Key đã dùng với payload khác                    | `{ "type":"conflict", "detail":"idempotency mismatch" }`   |
| 422  | Business rule (đã reject ở hard rule, vẫn trả response)    | `{ "type":"declined", "decision":"DECLINE", "rule":"bin_blocked" }` |
| 429  | Rate-limit                                                  | `Retry-After: 1`                                           |
| 503  | Degraded (model đang fail, fallback sang rule-only)         | `{ "type":"degraded", "mode":"rules-only" }`               |

#### 6.3.2 Control-plane: Rules & Policy

```
GET    /v1/rulesets                    # list
POST   /v1/rulesets                    # tạo (draft)
GET    /v1/rulesets/{id}               # chi tiết + YAML/DDL
PUT    /v1/rulesets/{id}               # cập nhật (tạo version mới)
POST   /v1/rulesets/{id}/validate      # dry-run trên test corpus
POST   /v1/rulesets/{id}/promote       # staging -> prod (audit trail)
GET    /v1/policies
POST   /v1/policies
PUT    /v1/policies/{id}               # cập nhật thresholds
```

**Ví dụ tạo rule set**

```http
POST /v1/rulesets
Authorization: Bearer <admin_jwt>
Content-Type: application/json
```

```json
{
  "name": "high_risk_us_v1",
  "version": 3,
  "policy_id": "policy_visa_domestic_v3",
  "rules": [
    {
      "id": "novel_device_high_amount",
      "priority": 100,
      "when": "model_xgb_score >= 0.55 AND device_age_hours < 24 AND amount > 500",
      "then": { "action": "CHALLENGE", "challenge_kind": "OTP" }
    },
    {
      "id": "velocity_15min",
      "priority": 90,
      "when": "tx_count_15m >= 8",
      "then": { "action": "DECLINE" }
    }
  ]
}
```

**Response 201 Created**

```json
{
  "id": "rs_01J7F...",
  "status": "draft",
  "wasm_url": "s3://fds-rules/high_risk_us_v1_v3.wasm",
  "created_at": "2026-07-09T07:20:00Z"
}
```

#### 6.3.3 Blacklist

```
GET    /v1/blacklist?kind=card_bin&value_hash=...
POST   /v1/blacklist
DELETE /v1/blacklist/{id}
```

**Body**

```json
{
  "kind": "device_fp",
  "value": "fp_01HZP2...",        // sẽ được tokenize/hash phía server
  "reason": "linked_to_fraud_ring_2024Q4",
  "expires_at": "2026-10-09T00:00:00Z"
}
```

**Response**

```json
{
  "id": 9182745,
  "kind": "device_fp",
  "value_hash": "sha256:9b1c...",
  "created_at": "2026-07-09T07:20:00Z",
  "expires_at": "2026-10-09T00:00:00Z",
  "created_by": "analyst@dat.co"
}
```

#### 6.3.4 Model Management

```
GET    /v1/models
GET    /v1/models/{id}
POST   /v1/models                    # upload artifact (multipart)
POST   /v1/models/{id}/promote      # { traffic_pct: 10 }
POST   /v1/models/{id}/rollback
GET    /v1/models/{id}/metrics
```

**Body promote**

```json
{ "traffic_pct": 10, "shadow": true, "guardrails": { "max_latency_p99_ms": 60 } }
```

#### 6.3.5 Override & Case Management

```
POST   /v1/decisions/{tx_id}/override   # từ REVIEW → APPROVE/DECLINE
GET    /v1/cases?status=open&assignee=analyst_a
POST   /v1/cases/{id}/notes
```

#### 6.3.6 Webhook Outbound

```
POST <merchant_webhook_url>
Headers:
  X-FDS-Event: decision.challenge
  X-FDS-Signature: t=<unix>,v1=<hmac_sha256>
  X-FDS-Delivery: <uuid>
Body:
{
  "tx_id": "...",
  "decision": "CHALLENGE",
  "challenge_kind": "OTP",
  "expires_at": "..."
}
```

Retry: exponential backoff `1s, 5s, 30s, 5m, 30m` (tối đa 5 lần), DLQ cuối cùng.

### 6.4 Quota & Rate-limit
- Per-merchant token bucket, refill 1000 RPS, burst 5000.
- 429 với header `Retry-After`, `X-RateLimit-Remaining`.
- Quota riêng cho admin API: 60 req/min/user.

### 6.5 The Ultra-Fast Python Stack (Tối ưu cho Máy cá nhân 16GB RAM & GTX 3050)
Nhằm mục đích hợp nhất ngôn ngữ giữa đội ngũ Backend và Data Science, hệ thống sẽ sử dụng **Python** làm cốt lõi, nhưng được tối ưu hóa để chạy nhẹ nhàng trên cấu hình cá nhân:

1. **Web Framework**: FastAPI (async/await).
2. **Event Loop**: uvloop (Cython, tối ưu CPU cực tốt).
3. **JSON Parser**: orjson (Rust, nhẹ RAM và siêu nhanh).
4. **Redis Client**: redis.asyncio (Pool bất đồng bộ).
5. **AI Inference**: `onnxruntime-gpu` nhúng trực tiếp hoặc Triton siêu nhẹ để tránh tốn quá nhiều VRAM của GTX 3050.

#### Cơ chế Singleflight chống Bão Cache & Double-Swipe
Mã nguồn thực chiến (`main.py`) tích hợp Async Singleflight, đảm bảo nếu hàng ngàn request cùng lúc miss cache trên 1 user, chỉ có 1 request đi truy vấn, các request khác đứng chờ. Hỗ trợ Redis pipelining để đếm số lượng giao dịch và đọc feature trong một round-trip.

```python
import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Callable

from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
import orjson
import redis.asyncio as aioredis
import tritonclient.grpc.aio as grpcclient
import simpleeval

# 1. Cơ chế Singleflight
class AsyncSingleFlight:
    def __init__(self):
        self._calls: Dict[str, asyncio.Future] = {}

    async def do(self, key: str, coro_func: Callable):
        if key in self._calls:
            return await self._calls[key]
        future = asyncio.Future()
        self._calls[key] = future
        try:
            result = await coro_func()
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            self._calls.pop(key, None)

singleflight = AsyncSingleFlight()

# 2. Khởi tạo tài nguyên
redis_pool = None
triton_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_pool, triton_client
    redis_pool = aioredis.Redis(host="localhost", port=6379, max_connections=5000)
    triton_client = grpcclient.InferenceServerClient(url="localhost:8001")
    yield
    await redis_pool.aclose()
    await triton_client.close()

app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse)

# 3. Hot-path
@app.post("/api/v1/score")
async def score_transaction(request: Request):
    start_time = time.perf_counter()
    body = await request.body()
    data = orjson.loads(body)
    tx_id, user_id, amount = data["tx_id"], data["user_id"], data["amount_minor"]

    if amount > 500_000_000:
        return ORJSONResponse({"tx_id": tx_id, "decision": "DECLINE", "reason": "AMOUNT_EXCEED"})

    async def fetch_and_incr():
        async with redis_pool.pipeline(transaction=False) as pipe:
            pipe.incr(f"vel:tx_count_10m:{user_id}")
            pipe.expire(f"vel:tx_count_10m:{user_id}", 600)
            pipe.hgetall(f"feat:profile:{user_id}")
            return await pipe.execute()

    results = await singleflight.do(f"fetch_{user_id}", fetch_and_incr)
    tx_count_10m = results[0]
    
    try:
        await asyncio.sleep(0.015) # Giả lập Triton
        ai_score = 0.85
    except asyncio.TimeoutError:
        ai_score = 0.5

    context = {"score": ai_score, "tx_count": tx_count_10m}
    if simpleeval.simple_eval("score > 0.8 or tx_count > 10", names=context):
        decision = "DECLINE"
    elif simpleeval.simple_eval("score > 0.6", names=context):
        decision = "CHALLENGE"
    else:
        decision = "APPROVE"

    latency_ms = (time.perf_counter() - start_time) * 1000
    return ORJSONResponse({
        "tx_id": tx_id, "decision": decision, "score": ai_score, "latency_ms": round(latency_ms, 2)
    })
```

#### Bí quyết chạy Production cho ứng dụng Python (Zero-Downtime, No OOM)
Sử dụng Gunicorn làm Process Manager với `uvicorn.workers.UvicornWorker` để tận dụng đa nhân CPU.
Công thức: `Số Worker = (Số CPU Cores x 2) + 1`

```bash
export PYTHONASYNCIODEBUG=0
export UVLOOP_ENABLED=1

# Máy cá nhân có thể để 3 workers để không ăn hết 16GB RAM
gunicorn main:app \
  --workers 3 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --max-requests 5000 \
  --max-requests-jitter 1000 \
  --log-level warning
```
- `--max-requests 10000`: Ngăn chặn rò rỉ RAM (OOM) bằng cách giết và sinh lại worker mới tự động mỗi khi xử lý xong 10,000 requests.
- `--log-level warning`: Tắt log HTTP `GET /` để tránh nghẽn I/O tại tốc độ 10,000 TPS. Gửi log/audit sự kiện qua Kafka bất đồng bộ thay vì in ra console.

*Lợi ích:* Với môi trường Local, stack này vẫn dư sức gánh hàng trăm TPS mà chỉ ngốn vài trăm MB RAM, tận dụng hoàn toàn GPU GTX 3050 thông qua ONNXRuntime nhúng hoặc TensorRT.

---

## 7. Mô hình AI & Feature Engineering

### 7.1 Feature categories (≈ 220 features)

| Category                          | Ví dụ                                                          | Nguồn                |
| --------------------------------- | -------------------------------------------------------------- | -------------------- |
| Transactional (raw)               | amount, currency, country, MCC, channel                       | hot-path             |
| Velocity                          | tx_count_{5m,10m,1h,24h}, amt_sum_{...}                       | Flink window         |
| Geo                               | distance_from_last_km, distinct_country_24h, ip_country        | Flink + Redis        |
| Device                            | device_age_days, distinct_users_24h_on_device                  | Redis                |
| Behavioral                        | avg_amount_30d, weekday_hour_histogram                         | Iceberg              |
| Merchant risk                     | chargeback_rate_30d, fraud_rate_30d, mcc_risk                  | Iceberg              |
| Network                           | ip_risk, asn_risk, vpn_flag                                    | 3rd-party            |
| Historical labels                 | past_chargebacks, past_challenge_count                         | Iceberg              |
| Graph (GNN)                       | 2-hop neighbor stats: avg_risk, max_risk                       | Neo4j/Feature store  |

### 7.2 Loss & Metrics
- XGBoost: binary log-loss; class imbalance xử lý bằng `scale_pos_weight` + focal loss thử nghiệm.
- Autoencoder: reconstruction loss (MSE) trên transactional features.
- GNN: GraphSAGE với neighbor sampling 2-hop, output 1 logit.
- Blender: logistic regression trên `[s_xgb, err_ae, s_gnn, …]`; có thể là 1 NN nhỏ.

**Offline metrics**
- AUC, AUC-PR, recall @ FPR={0.5 %, 1 %, 2 %}.
- Calibration (reliability diagram, ECE).
- Drift: PSI/KL giữa production và training distribution theo từng feature.

**Online guardrails**
- Latency p99 ≤ 50 ms (rolling 5 phút).
- Score distribution drift (`KL > 0.1` ⇒ alert).
- Decision-mix drift (tỉ lệ DECLINE lệch > 1.5× so với baseline ⇒ alert).

### 7.3 Training & Retraining
- **Daily** batch retrain với last-30-day labels; so sánh champion vs challenger.
- **Streaming** partial-fit (river/online XGBoost) cho Autoencoder anomaly threshold.
- **Shadow** mode trên 1–5 % traffic, không ảnh hưởng decision.

### 7.4 Feature Store schema versioning
- Mỗi feature có `feature_definition.proto` với `version` & `owner`.
- `feature_set_vN.json` liệt kê các feature + dtype + null handling.
- Compatibility: bổ sung OK; rename/remove → bump major; pre-commit check tự động.

---

## 8. Bảo mật & Tuân thủ

| Lĩnh vực               | Biện pháp                                                                                |
| ----------------------- | ---------------------------------------------------------------------------------------- |
| Network                 | mTLS (SPIFFE), WAF, IP allow-list cho admin API, private link cho managed services.       |
| Identity                | OAuth2/OIDC + MFA cho admin; workload identity cho service-to-service.                    |
| Secret                  | HashiCorp Vault / KMS, secret rotation 30 ngày.                                            |
| Data                    | PII tokenization ở edge (FPE/AES-GCM); hashing cho blacklist keys; không log PII.        |
| Encryption at rest      | AES-256 (LUKS / KMS-managed).                                                            |
| Encryption in transit   | TLS 1.3, HTTP/2 cho REST; HTTP/2 + TLS cho gRPC.                                          |
| Compliance              | PCI-DSS (CDE scope cô lập), PSD2/SCA khi áp dụng CHALLENGE, GDPR right-to-erasure.       |
| Audit                   | Mọi thay đổi rule/policy/model có actor, ts, before/after, trace-id.                      |
| Threat model            | STRIDE hàng quý; red-team giả lập gian lận vòng lặp, velocity abuse, ATO.                  |
| Isolation               | Network policy zero-trust trong cluster; namespace riêng cho inference, training.        |

---

## 9. Observability & SRE

### 9.1 Metrics (Prometheus)
- `fds_request_total{decision,merchant_tier}` (counter)
- `fds_request_latency_ms{stage=ingress|feature|inference|rules|total}` (histogram)
- `fds_score_distribution` (histogram)
- `fds_model_inference_seconds{model}` (histogram)
- `fds_redis_get_seconds{status}` (histogram)
- `fds_kafka_consumer_lag{topic,partition}`
- `fds_rule_fires_total{rule_id}`
- `fds_circuit_breaker_state{component}`

**SLOs**
- Availability 99.99 % ⇒ error budget 4.32 phút/tháng.
- Latency p99 < 50 ms ⇒ budget 0.5 % request quá hạn.
- Decision-quality ⇒ FPR drift ≤ 1.5× baseline; AUC giảm ≤ 1 %.

### 9.2 Tracing (OpenTelemetry)
- Trace context truyền từ `tx_id`.
- Spans: `ingress → hard_rules → feature_fetch → model:xgb → model:aae → model:gnn → blender → soft_rules → decision → egress → kafka_publish`.
- Sampling 100 % lỗi, 1 % happy-path, head-based + tail-based.

### 9.3 Logs (ELK / Loki)
- Structured JSON, mức INFO/WARN/ERROR.
- Correlation: `trace_id`, `tx_id`, `user_id_hash`, `merchant_id`, `model_version`.
- Sampling: 100 % error, 10 % warn, 1 % info.
- 90 ngày hot, 1 năm warm, 7 năm archive.

### 9.4 Alerting
| Alert                              | Điều kiện                                                  | Hành động                                |
| ---------------------------------- | ---------------------------------------------------------- | ---------------------------------------- |
| `LatencyP99Breach`                 | p99 > 50 ms trong 5 phút                                  | Auto-scale inference pods; page on-call   |
| `ErrorRateSpike`                   | 5xx > 0.5 % trong 3 phút                                  | Enable degraded mode (rules-only); page  |
| `RedisDown`                        | L1 unavailable > 10 s                                     | Switch L2 (DynamoDB); broadcast          |
| `ScoreDrift`                       | KL > 0.1                                                  | Pause model promotion; review            |
| `DecisionMixShift`                 | DECLINE% > 1.5× baseline                                   | Investigate                                |
| `KafkaLag`                         | lag > 100 k                                                | Scale Flink; backfill                     |
| `GpuOom`                           | Triton OOM kill > 3/giờ                                    | Reduce dynamic batch; rollback model      |

### 9.5 Chaos & DR
- GameDay hàng tháng: kill Redis, kill 1 AZ, inject 200 ms latency ở Redis.
- DR runbook: RTO 15 phút, RPO 5 phút; standby cluster ở region khác, traffic shift qua Route53.

---

## 10. Triển khai & Lộ trình

### 10.1 Tech stack đề xuất

| Layer                | Tech                                                                              |
| -------------------- | --------------------------------------------------------------------------------- |
| Container            | Docker / Podman                                                                   |
| Orchestrator         | Kubernetes (EKS/GKE/on-prem) với Karpenter autoscaling                            |
| Service mesh         | Istio/Linkerd (mTLS, telemetry)                                                   |
| API gateway          | Envoy + custom filters, hoặc Kong                                                  |
| Stream               | Apache Kafka 3.7 (KRaft), Apache Flink 1.19                                        |
| Feature store        | Redis 7 Cluster, ScyllaDB cho cold                                                |
| Inference            | NVIDIA Triton 2.45, ONNX Runtime, TensorRT                                         |
| Backend services     | Go (inference/rules/decision), Rust cho hot-utils, Python cho training            |
| DB                   | PostgreSQL 16, ClickHouse 24                                                       |
| Lakehouse            | Apache Iceberg, Apache Spark, Trino                                               |
| CI/CD                | GitHub Actions / Argo CD, Helm + Kustomize                                         |
| IaC                  | Terraform / Pulumi                                                                 |
| GPU                  | A10G / L4 cho inference; A100 cho training                                         |

### 10.2 Sơ đồ triển khai (Kubernetes)

```
namespace: fds-prod
├─ deploy/decision-orchestrator    (Go, HPA, 6-60 pods, 0.5 vCPU, 768 MB RAM)
├─ deploy/scoring-service          (Go, HPA, 6-40 pods, 1 vCPU, 1.5 GB RAM)
├─ deploy/feature-svc              (Go, 4 pods, 0.5 vCPU, 1 GB RAM)
├─ deploy/rules-engine             (Rust+Wasmtime, 4 pods, 1 vCPU, 1 GB RAM)
├─ deploy/triton-inference         (GPU, 2-12 pods, 1 GPU A10G, 8 vCPU, 16 GB)
├─ deploy/flink-jobmanager         (1 active + 1 standby, 2 vCPU, 4 GB)
├─ deploy/flink-taskmanager        (8-32 pods, 2 vCPU, 4 GB)
├─ sts/redis                       (6 nodes cluster)
├─ sts/kafka                       (3 brokers + 3 controllers)
└─ hpa/profiles based on prometheus-adapter
```

### 10.3 Lộ trình (18 tuần)

| Tuần  | Cụm công việc                                                             | Tiêu chí nghiệm thu                                                                                              |
| ----- | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| 1–2   | **Foundation**: infra (K8s, Kafka, Postgres), CI/CD, skeleton service, OTel | `hello-fds` gRPC service + grafana dashboard cơ bản, MTLS nội bộ.                                                  |
| 3–4   | **Hot path v0**: gRPC API → rules engine (DSL) → hard-coded decision       | 100 RPS end-to-end, latency < 50 ms.                                                                                |
| 5–6   | **Feature store**: Redis + Flink, 5 velocity/geo features                  | 50 features online, p99 fetch < 5 ms.                                                                              |
| 7–8   | **Model v1**: XGBoost on tabular, ONNX, Triton                              | AUC ≥ 0.93 trên holdout; serving latency < 3 ms.                                                                   |
| 9–10  | **Decision orchestration**: combine strategies, soft rules                | Schema decision policy version đầy đủ, A/B 5 %.                                                                    |
| 11–12 | **Model v2**: Autoencoder + GNN, ensemble, blender                         | AUC ≥ 0.96; recall@1%FPR ≥ 0.90.                                                                                   |
| 13–14 | **Audit & Lakehouse**: Kafka → Iceberg, ClickHouse dashboards              | Dashboard ops/risk cho C-level.                                                                                    |
| 15–16 | **Hardening**: chaos, DR runbook, security review, load-test 5k TPS        | Pass chaos day; load test bền vững 24h.                                                                            |
| 17–18 | **GA**: canary 5 % → 25 % → 100 %, handoff, KPI tracking 30 ngày          | FPR ≤ 1 %, recall ≥ 95 %, chargeback loss giảm ≥ 30 %.                                                              |

### 10.4 Cost model (ước lượng sơ bộ)
- Compute (K8s + GPU): tỉ trọng lớn nhất; reservation 1 năm cho Triton.
- Kafka & Flink giữ chi phí vừa; Iceberg/S3 giá rẻ cho cold storage.
- Tối ưu: model distillation để giảm GPU footprint; spot instance cho training.

---

## 11. Phụ lục

### 11.1 Thuật ngữ
- **TPS** — Transactions Per Second.
- **CDE** — Cardholder Data Environment (PCI-DSS).
- **AE/Autoencoder** — mô hình học không giám sát phát hiện bất thường.
- **GNN** — Graph Neural Network, mô hình trên đồ thị.
- **WAL/SCD2** — Slowly Changing Dimension type 2.
- **CRDT** — Conflict-free Replicated Data Type.
- **MTLS** — Mutual TLS.
- **SHAP/LIME** — giải thích mô hình (dùng cho reviewer dashboard).

### 11.2 Mẫu gRPC IDL (rút gọn)

```protobuf
syntax = "proto3";
package fds.scoring.v1;

service ScoringService {
  rpc Score(ScoreRequest) returns (ScoreResponse);
  rpc ScoreBatch(BatchScoreRequest) returns (BatchScoreResponse);
}

message ScoreRequest {
  string tx_id         = 1;
  string user_id       = 2;
  string card_bin      = 3;
  string device_fp     = 4;
  string merchant_id   = 5;
  string mcc           = 6;
  string channel       = 7;
  int64  amount_minor  = 8;
  string currency      = 9;
  string country       = 10;
  double lat           = 11;
  double lon           = 12;
  int64  ts_ms         = 13;
  map<string,string> attributes = 14;
  string policy_id     = 15;
}

enum Decision { UNKNOWN=0; APPROVE=1; CHALLENGE=2; DECLINE=3; REVIEW=4; }
enum ChallengeKind { NONE=0; OTP=1; THREE_DS=2; BIOMETRIC=3; PIN=4; }

message ScoreResponse {
  string tx_id          = 1;
  Decision decision     = 2;
  ChallengeKind challenge_kind = 3;
  double score          = 4;
  repeated string rules_fired = 5;
  string rule_set_version = 6;
  map<string,string> model_versions = 7;
  int32  latency_ms     = 8;
  string trace_id       = 9;
}

message BatchScoreRequest { repeated ScoreRequest requests = 1; }
message BatchScoreResponse { repeated ScoreResponse responses = 1; }
```

### 11.3 Ma trận RACI (rút gọt)
| Quyết định                  | Risk/Compliance | Data Eng | ML Eng | SRE | Product |
| --------------------------- | --------------- | -------- | ------ | --- | ------- |
| Promote model lên prod      | C               | I        | R      | A   | C       |
| Thay đổi threshold policy   | C               | I        | C      | I   | R       |
| Thay đổi hard-rule blacklist | A              | I        | C      | I   | R       |
| Sửa p99 SLA                 | I               | C        | C      | R   | A       |

### 11.4 Rủi ro & giảm thiểu
| Rủi ro                              | Xác suất | Tác động | Giảm thiểu                                                   |
| ----------------------------------- | -------- | -------- | ------------------------------------------------------------- |
| Concept drift sau re-open economy  | Cao      | Trung bình | Recurring training, drift monitoring, manual fallback.       |
| Adversarial fraud ring              | Trung bình | Cao      | GNN + blacklist + share intelligence consortium.             |
| GPU shortage                        | Thấp     | Cao      | CPU-only fallback (XGBoost only), multi-cloud.              |
| Data breach PII                     | Thấp     | Rất cao | Tokenization, encryption, audit, PCI attestation.            |
| False positive mùa lễ              | Cao      | Trung bình | Holiday policy override, override queue SLA.                |
| Vendor lock-in (cloud)              | Trung bình | Trung bình | Helm IaC portable, multi-cloud-ready schema.                |

### 11.5 Tài liệu tham chiếu
- NIST SP 800-63B (Digital Identity).
- PCI DSS v4.0.
- PSD2 RTS on SCA & CSC.
- Apache Kafka & Flink official docs.
- NVIDIA Triton Inference Server Best Practices.
- Google SRE Book — SLO chapter.

---

**Kết luận:** Bản thiết kế này cân bằng giữa **độ trễ cực thấp (< 50 ms p99)**, **độ chính xác cao (recall ≥ 95 %)** và **khả năng vận hành (observability, chaos, DR)**. Kiến trúc 4 lớp (Stream → Feature → Inference → Decision) phù hợp với tiêu chuẩn doanh nghiệp, có thể triển khai tuần tự theo lộ trình 18 tuần để đạt GA, đồng thời cho phép mở rộng sang nhiều region/tenant khi tăng trưởng.
