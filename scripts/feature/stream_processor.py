import os
import logging
from quixstreams import Application
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 1. Khởi tạo kết nối Redis
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# 2. Khởi tạo Quix Application
broker = os.environ.get("KAFKA_BROKERS", "localhost:19092")
app = Application(
    broker_address=broker,
    consumer_group="fds-stream-processor-group",
    auto_offset_reset="earliest" # Dùng earliest để consume data cũ khi test. Khi prod chạy có thể dùng latest.
)

input_topic = app.topic("fds.tx.raw.v1", value_deserializer="json")

# Sử dụng StreamingDataframe
sdf = app.dataframe(input_topic)

def calculate_realtime_features(row: dict):
    """
    Hàm xử lý từng message. Tính toán counter và ghi trực tiếp vào Redis (Write-through).
    Trong mô hình phân tán thật sự, Quix/Faust sẽ lưu state ở RocksDB nội bộ. 
    Tuy nhiên, để cực nhẹ cho bản Local và để API (FastAPI) có thể query ngay, ta đẩy thẳng vào Redis.
    """
    user_id = row.get("user_id")
    if not user_id:
        return row
        
    amount = row.get("amount_minor", 0) / 100.0
    
    # Key lưu trữ real-time features trên Redis
    key = f"rt_features:{user_id}"
    
    try:
        # Sử dụng Redis Pipeline để tối ưu I/O (chạy nhiều lệnh 1 lượt)
        pipeline = redis_client.pipeline()
        pipeline.hincrby(key, "tx_count_10m", 1)
        pipeline.hincrbyfloat(key, "amt_sum_1h", amount)
        pipeline.expire(key, 3600) # Key tự xóa sau 1 giờ
        pipeline.execute()
    except Exception as e:
        logger.error(f"Redis error for user {user_id}: {e}")
        
    return row

def log_transaction(row: dict):
    # Log nhẹ để theo dõi
    tx_id = row.get('tx_id', 'unknown')
    user_id = row.get('user_id', 'unknown')
    amount = row.get('amount_minor', 0) / 100.0
    logger.info(f"Processed TX {tx_id} | User: {user_id[:8]}... | Amt: {amount}")
    return row

# 3. Chuỗi biến đổi (Pipeline Topology)
sdf = sdf.apply(calculate_realtime_features)
sdf = sdf.apply(log_transaction)

if __name__ == "__main__":
    logger.info(f"Starting Stream Processor. Connecting to {broker}...")
    logger.info(f"Redis connected: {redis_client.ping()}")
    # Chạy vòng lặp consume
    app.run()
