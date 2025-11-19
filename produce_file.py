import time
import json
import pandas as pd
from kafka import KafkaProducer

# --- CẤU HÌNH ---
KAFKA_TOPIC = 'test' 
KAFKA_SERVER = 'localhost:9092'
FILE_PATH = './data/raw_data.parquet' 

# Hàm chuyển đổi dữ liệu sang JSON (xử lý ngày tháng để không bị lỗi)
def json_serializer(data):
    return json.dumps(data, default=str).encode('utf-8')

print(f"🔄 Đang kết nối đến Kafka tại {KAFKA_SERVER}...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=json_serializer
)

print(f"📖 Đang đọc file Parquet: {FILE_PATH}...")
try:
    # Đọc file
    df = pd.read_parquet(FILE_PATH)
    
    # --- QUAN TRỌNG: Ép kiểu datetime thành chuỗi (String) ---
    # ClickHouse nhận String '2023-01-01...' và tự chuyển thành DateTime
    if 'datetime' in df.columns:
        df['datetime'] = df['datetime'].astype(str)

    print(f"✅ Đã đọc xong {len(df)} dòng. Bắt đầu gửi...")
    
    # Chuyển thành list dictionary để gửi
    records = df.to_dict(orient='records')

    for i, row in enumerate(records):
        producer.send(KAFKA_TOPIC, value=row)
        
        # In log mỗi 2000 dòng để theo dõi
        if i % 2000 == 0:
            # In thử tọa độ để chắc chắn dữ liệu đúng
            print(f"Sent dòng {i}: Xe {row.get('vehicle')} tại {row.get('lat')}, {row.get('lng')}")
            
        # Chạy nhanh hay chậm? 
        # Nếu muốn nhìn trên bản đồ di chuyển từ từ, hãy bỏ comment dòng dưới:
        # time.sleep(0.01) 

    print(f"\n🎉 Hoàn tất! Đã gửi {len(records)} tin nhắn.")

except Exception as e:
    print(f"❌ Lỗi: {e}")
finally:
    producer.flush()
    producer.close()