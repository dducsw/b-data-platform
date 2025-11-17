from kafka import KafkaProducer
import csv
import json
from tqdm import tqdm

# =========================================
# Kafka config
# =========================================

# Host (máy của bạn) nói chuyện với Kafka qua 9092 & 9094 (đã map trong docker-compose)
BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9094']

TOPIC_NAME = "bus_gps_raw"
CSV_FILE = "./data/raw_2025-04-01.csv"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    linger_ms=50,
    batch_size=32768,
    acks="all",
    retries=5,
)


# =========================================
# Send CSV → Kafka
# =========================================
def send_csv_to_kafka():
    total_sent = 0

    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in tqdm(reader, desc="Sending to Kafka"):

            # Map schema rõ ràng theo consumer.py (9 fields)
            payload = {
                "datetime": row.get("datetime"),
                "date": row.get("date"),
                "vehicle": row.get("vehicle"),
                "lng": row.get("lng"),
                "lat": row.get("lat"),
                "driver": row.get("driver"),
                "speed": row.get("speed"),
                "door_up": row.get("door_up"),
                "door_down": row.get("door_down"),
            }

            # vehicle làm key → giúp Kafka hash ra partition
            key = str(payload["vehicle"]) if payload["vehicle"] else "unknown"

            producer.send(
                TOPIC_NAME,
                key=key,
                value=payload
            )

            total_sent += 1

        # đảm bảo gửi hết
        producer.flush()

    print(f"✅ Done! Sent total {total_sent} records to topic '{TOPIC_NAME}'.")


# =========================================
# Main
# =========================================
if __name__ == "__main__":
    print("🚀 Starting CSV → Kafka producer...")
    try:
        send_csv_to_kafka()
    finally:
        # đóng producer gọn, tránh lỗi thread sau khi kết thúc chương trình
        producer.close()
        print("👋 Producer closed.")
