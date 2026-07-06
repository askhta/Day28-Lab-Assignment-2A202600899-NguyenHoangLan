# prefect/flows/kafka_to_delta.py
"""Integration 2: Kafka → Prefect → Delta Lake.

Chạy từ máy host (mặc định):
    python kafka_to_delta.py run     # chạy flow 1 lần ngay lập tức
    python kafka_to_delta.py         # deploy + serve với schedule 5 phút/lần

Chạy trong prefect-worker container: env KAFKA_BOOTSTRAP=kafka:29092,
DELTA_PATH=/opt/delta-lake/raw đã được set sẵn trong docker-compose.yml.
"""
from prefect import flow, task
from kafka import KafkaConsumer
import json, os, sys
import pandas as pd
from datetime import datetime

# Host chạy trực tiếp → localhost:9092; trong Docker network → kafka:29092
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DELTA_PATH = os.environ.get("DELTA_PATH", "delta-lake/raw")


@task(retries=2, retry_delay_seconds=5)
def consume_and_process():
    """Consume data from Kafka topic"""
    consumer = KafkaConsumer(
        "data.raw",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="delta-writer",
        enable_auto_commit=True,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode()),
    )
    records = [msg.value for msg in consumer]
    consumer.close()

    print(f"Consumed {len(records)} records from Kafka ({KAFKA_BOOTSTRAP})")
    return records


@task
def save_to_delta(records):
    """Save records to Delta Lake (parquet format)"""
    if not records:
        print("No records to save")
        return

    df = pd.DataFrame(records)
    # Giả lập Delta Lake bằng parquet (local volume)
    os.makedirs(DELTA_PATH, exist_ok=True)
    out = f"{DELTA_PATH}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(out)
    print(f"Saved {len(df)} records to Delta Lake: {out}")


@flow(name="Kafka to Delta Pipeline")
def kafka_to_delta_flow():
    """Main flow: consume from Kafka and save to Delta Lake"""
    records = consume_and_process()
    save_to_delta(records)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        # Chạy 1 lần ngay để test integration
        kafka_to_delta_flow()
    else:
        # Deploy + serve: đăng ký deployment với Prefect server và chạy
        # theo schedule mỗi 5 phút (process này phải giữ chạy)
        kafka_to_delta_flow.serve(
            name="kafka-to-delta",
            cron="*/5 * * * *",
        )
