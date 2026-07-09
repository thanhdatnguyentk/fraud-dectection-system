import os
import time
import logging
import argparse
import pandas as pd
from quixstreams import Application

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def produce_transactions(file_path: str, tps: int = 100, max_msgs: int = 0):
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}. Please generate it first.")
        return

    broker = os.environ.get("KAFKA_BROKERS", "localhost:19092")
    logger.info(f"Connecting to Redpanda at {broker}...")
    
    app = Application(broker_address=broker)
    topic = app.topic("fds.tx.raw.v1", value_serializer="json")
    
    logger.info(f"Reading dataset: {file_path}")
    df = pd.read_parquet(file_path)
    
    sleep_time = 1.0 / tps
    
    with app.get_producer() as producer:
        logger.info(f"Starting to produce messages to fds.tx.raw.v1 at {tps} TPS...")
        count = 0
        for _, row in df.iterrows():
            # Convert to dict and handle NaNs for JSON serialization
            record = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            user_id = str(record.get("user_id", ""))
            
            # Serialize the message
            msg = topic.serialize(key=user_id, value=record)
            
            # Produce
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            
            count += 1
            if count % tps == 0:
                logger.info(f"Produced {count} messages...")
                
            if max_msgs > 0 and count >= max_msgs:
                break
                
            time.sleep(sleep_time)
            
    logger.info(f"Done. Total produced: {count} messages.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, default="data/canonical/sample.parquet", help="Path to canonical parquet file")
    parser.add_argument("--tps", type=int, default=100, help="Transactions per second")
    parser.add_argument("--max", type=int, default=0, help="Max messages to produce (0 = all)")
    args = parser.parse_args()
    
    produce_transactions(args.file, args.tps, args.max)
