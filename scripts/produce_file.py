import json
import time
import pyarrow.parquet as pq
from kafka import KafkaProducer

# 1. Cấu hình Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    api_version=(0, 10, 1),
    # Tối ưu hóa gửi: Tăng kích thước bộ đệm để gửi nhanh hơn
    batch_size=16384, 
    linger_ms=5
)

TOPIC_NAME = 'bus_gps_protobuf'
FILE_PATH = '../data/tostream10M.parquet'

print(f"📖 Đang mở file Parquet: {FILE_PATH}...")

try:
    # Mở file Parquet ở chế độ stream (không load hết vào RAM)
    parquet_file = pq.ParquetFile(FILE_PATH)
    
    total_sent = 0
    start_time = time.time()

    # Đọc từng gói (Batch), ví dụ mỗi gói 50.000 dòng
    for batch in parquet_file.iter_batches(batch_size=50000):
        # Chuyển batch thành Pandas DataFrame nhỏ
        df_batch = batch.to_pandas()
        
        # Chuyển đổi kiểu dữ liệu một lần cho cả batch (Nhanh hơn làm từng dòng)
        # Ép kiểu string cho datetime và vehicle
        df_batch['datetime'] = df_batch['datetime'].astype(str)
        df_batch['vehicle'] = df_batch['vehicle'].astype(str)
        
        # Chuyển toàn bộ batch thành List of Dictionaries (Cực nhanh)
        records = df_batch.to_dict('records')

        # Duyệt qua list dict (Nhanh hơn iterrows rất nhiều)
        for row in records:
            data = {
                "log_time": row['datetime'],
                "vehicle_plate": row['vehicle'],
                "lng": float(row['lng']),
                "lat": float(row['lat']),
                "speed": float(row['speed']) if row['speed'] is not None else 0.0
            }
            
            producer.send(TOPIC_NAME, value=data)
            total_sent += 1

        # In tiến độ sau mỗi batch
        print(f"🚀 Đã gửi batch xong. Tổng cộng: {total_sent} dòng...")

    producer.flush()
    end_time = time.time()
    duration = end_time - start_time
    print(f"✅ Hoàn tất! Đã gửi {total_sent} dòng trong {duration:.2f} giây.")

except Exception as e:
    print(f"❌ Lỗi: {e}")