# Kế hoạch triển khai Hệ thống Phát hiện Gian lận (Local / Personal Edition)
# Implementation Plan for Local FDS (16GB RAM, GTX 3050)

> **Mục tiêu:** Chuyển hóa bản thiết kế `fraud-detection-system-design.md` thành mã nguồn thực tế, có thể chạy mượt mà trên 1 laptop cá nhân qua `docker-compose`.

---

## Giai đoạn 1: Khởi tạo Hạ tầng Cục bộ (Local Infrastructure)
**Thời gian dự kiến:** 2 - 3 ngày

**Mục tiêu:** Dựng các Database và Message Broker cực nhẹ thông qua Docker Compose.
- **Tạo file `docker-compose.yml` bao gồm:**
  1. **Redpanda:** Chạy 1 node duy nhất (thay thế Kafka). Thiết lập auto-create topics.
  2. **Redis:** Cấu hình chuẩn không cần cluster, cấu hình `maxmemory 2gb` và `maxmemory-policy allkeys-lru` để tránh tràn RAM.
  3. **PostgreSQL 16:** Lưu trữ cấu hình Rules và Audit log.
  4. (Tùy chọn) **Prometheus + Grafana:** Cấu hình cực kỳ cơ bản (scrape interval 15s) để vẽ biểu đồ TPS và Latency.
- **Xây dựng Repository:**
  - Setup Poetry hoặc `uv` (khuyến nghị `uv` cho tốc độ siêu nhanh).
  - Cấu hình `.env.local` chứa chuỗi kết nối cục bộ.

## Giai đoạn 2: Xây dựng Ingestion & Stream Processing (Pipeline)
**Thời gian dự kiến:** 4 - 5 ngày

**Mục tiêu:** Chạy giả lập luồng dữ liệu giao dịch và tính toán đặc trưng (Feature) theo thời gian thực.
- **Data Generator:** Viết script `kafka_producer.py` (chuyển qua đọc file Parquet nội bộ) đẩy giao dịch liên tục vào Redpanda topic `fds.tx.raw.v1` với tốc độ 100-500 TPS.
- **Stream Processor (Faust/Quix):**
  - Khởi tạo 1 consumer đọc từ `fds.tx.raw.v1`.
  - Tính toán các counter thời gian thực (vd: `tx_count_10m`, `amount_sum_1h`).
  - Ghi thẳng các features này vào **Redis** (Write-through).

## Giai đoạn 3: Feature Engineering Offline & Model Training
**Thời gian dự kiến:** 5 - 7 ngày

**Mục tiêu:** Tái tạo quá khứ để huấn luyện Mô hình ML trên tập IEEE-CIS.
- **ETL với Polars & DuckDB:**
  - Viết script chuyển đổi dataset raw CSV thành `tx_canonical.v1` lưu dưới định dạng **Parquet**.
  - Dùng DuckDB chạy các Window functions (SQL) để tạo tập `fact_features_offline.parquet`.
- **Huấn luyện mô hình:**
  - Train **XGBoost** model (phát hiện gian lận dựa trên features tabular).
  - Xuất mô hình ra định dạng **ONNX (FP16)** để tối ưu dung lượng VRAM (chỉ tốn vài MB trên GTX 3050).
- **Backfill:** Script dùng pipeline của `redis.asyncio` đẩy tập offline features vào Redis để chuẩn bị cho Scoring.

## Giai đoạn 4: Xây dựng Ultra-Fast Python API (Scoring Engine)
**Thời gian dự kiến:** 5 - 7 ngày

**Mục tiêu:** Trái tim của hệ thống, xử lý request và ra quyết định dưới 50ms.
- **Setup FastAPI:**
  - Cài đặt `FastAPI`, `uvloop`, `orjson`.
  - Viết endpoint `POST /api/v1/score`.
- **Tối ưu Hot-path:**
  - Triển khai **Async Singleflight** (class đã thiết kế) để chống bão truy vấn cùng 1 user_id.
  - Sử dụng Redis Pipeline (`MGET`, `HGETALL`) để kéo feature siêu tốc.
- **Nhúng ONNX Runtime:**
  - Khởi tạo `InferenceSession` của ONNXRuntime sử dụng `CUDAExecutionProvider` (vào thẳng GPU GTX 3050).
  - Đẩy tensor vào model ngay trong luồng async (hoặc dùng `run_in_executor` nếu model block nhẹ).
- **Rules Engine:**
  - Áp dụng module `simpleeval`.
  - Định nghĩa tập Hard-Rules (vd: blacklist BIN, amount > threshold) và Soft-Rules (kết hợp Score AI + Policy) bằng string expressions.

## Giai đoạn 5: Tích hợp, Load Test & Báo cáo
**Thời gian dự kiến:** 3 - 4 ngày

**Mục tiêu:** Chứng minh hệ thống đạt mọi chỉ tiêu thiết kế.
- **Integration Test:** Đảm bảo toàn bộ luồng từ Producer -> Redpanda -> FastAPI -> Redis -> ONNX -> Decision hoạt động trơn tru.
- **Load Test (Stress Test):**
  - Chạy `kafka_producer.py` đẩy tốc độ lên 500 TPS liên tục trong 10 phút.
  - Mở Grafana theo dõi lượng RAM (Target: < 2GB) và VRAM (Target: < 1GB).
  - Kiểm tra `latency_ms` trong API response đảm bảo p99 < 50ms.
- **Kịch bản tấn công (Scenario Injection):** Chạy `test_scenarios.py` giả lập 1 thẻ quẹt 30 lần trong 1 phút (Velocity attack) để xem Rules Engine có lập tức `DECLINE` hay không.

---
**Tóm tắt các Checkpoint quan trọng (Milestones):**
- [ ] Milestone 1: Docker Compose Up thành công (Redpanda + Redis + Postgres).
- [ ] Milestone 2: Producer đẩy được 500 msg/s vào Redpanda mà không lag.
- [ ] Milestone 3: Model XGBoost xuất thành công ra `model.onnx`.
- [ ] Milestone 4: FastAPI kéo feature, chạy ONNX, đánh giá Rule trong < 10ms.
- [ ] Milestone 5: API hoạt động ổn định ở 500 TPS mà không tràn RAM 16GB.
