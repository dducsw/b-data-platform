import pandas as pd

FILE_PATH = './data/raw_data.parquet'

print("Đang đọc file...")
df = pd.read_parquet(FILE_PATH)

# Lọc các dòng có lat hoặc lng bằng 0 hoặc null
missing_data = df[
    (df['lat'] == 0) | (df['lng'] == 0) | 
    (df['lat'].isnull()) | (df['lng'].isnull())
]

if not missing_data.empty:
    print(f"⚠️ Phát hiện {len(missing_data)} dòng bị lỗi vị trí!")
    print("Danh sách các xe bị lỗi:")
    print(missing_data['vehicle'].unique())
else:
    print("✅ Dữ liệu trong file sạch, không có tọa độ 0 hoặc Null.")