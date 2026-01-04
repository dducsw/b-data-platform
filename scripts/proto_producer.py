import pandas as pd
import base64
from kafka import KafkaProducer
from tqdm import tqdm

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9094'],
    value_serializer=lambda v: v,  # Giữ nguyên dữ liệu thô dạng byte
    linger_ms=10,
    batch_size=16384
)

# docker exec -it kafka-broker-1 kafka-topics.sh `
#   --bootstrap-server kafka-broker-1:29092 `
#   --create `
#   --topic bus_gps_protobuf `
#   --partitions 2 `
#   --replication-factor 2

parquet_file = './data/base64_60M.parquet'
topic_name = 'bus_gps_protobuf'

# Đọc file Parquet
df = pd.read_parquet(parquet_file)


# Đẩy dữ liệu lên Kafka
for idx, row in tqdm(df.iterrows(), total=len(df), desc="Sending protobuf to Kafka"):
    raw_base64 = row['raw_base64']
    raw_bytes = base64.b64decode(raw_base64)
    producer.send(topic_name, value=raw_bytes)

    print("Row count: ",idx)

producer.flush()
print("Data sent to Kafka successfully!")




