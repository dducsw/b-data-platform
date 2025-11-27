# decode_base64_parquet_to_files.py
import sys
import base64

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, FloatType, BooleanType, LongType, IntegerType,
    ArrayType,
)

sys.path.append("/opt/spark/data")
import ufms_pb2
from google.protobuf.message import DecodeError

# --------- CONFIG---------
# Parquet chứa cột base64
INPUT_FILE = "/opt/spark/data/base64_10M.parquet"
COLUMN_NAME = "raw_base64"        

OUTPUT_CSV_DIR = "/opt/spark/data/decoded_busgps_table_csv_udf"
OUTPUT_PARQUET_DIR = "/opt/spark/data/decoded_busgps_table_parquet_udf"


# ===== schema cho 1 bản ghi BusGPS =====
busgps_schema = StructType([
    StructField("msg_type", IntegerType(), True),
    StructField("vehicle", StringType(), True),
    StructField("driver", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("datetime", LongType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("z", FloatType(), True),
    StructField("heading", FloatType(), True),
    StructField("ignition", BooleanType(), True),
    StructField("aircon", BooleanType(), True),
    StructField("door_up", BooleanType(), True),
    StructField("door_down", BooleanType(), True),
    StructField("sos", BooleanType(), True),
    StructField("working", BooleanType(), True),
    StructField("analog1", FloatType(), True),
    StructField("analog2", FloatType(), True),
])


def _wp_to_dict(msg_type_value: int, wp) -> dict:
    """Map 1 BusWayPoint protobuf -> dict theo schema trên."""
    return {
        "msg_type": msg_type_value,
        "vehicle": wp.vehicle,
        "driver": wp.driver,
        "speed": wp.speed,
        "datetime": wp.datetime,
        "x": wp.x,
        "y": wp.y,
        "z": wp.z,
        "heading": wp.heading,
        "ignition": wp.ignition,
        "aircon": wp.aircon,
        "door_up": wp.door_up,
        "door_down": wp.door_down,
        "sos": wp.sos,
        "working": wp.working,
        "analog1": wp.analog1,
        "analog2": wp.analog2,
    }


def decode_busgps_from_b64(b64_str):

    if b64_str is None:
        return []

    try:
        raw_bytes = base64.b64decode(b64_str)
    except Exception:
        return []

    msg = ufms_pb2.BaseMessage()
    try:
        msg.ParseFromString(raw_bytes)
    except (DecodeError, Exception):
        return []

    rows = []
    msg_type_value = int(msg.msgType)  # enum -> int

    # 1) Trường đơn: msgBusWayPoint
    if msg.HasField("msgBusWayPoint"):
        wp = msg.msgBusWayPoint
        rows.append(_wp_to_dict(msg_type_value, wp))

    # 2) Trường repeated: msgBusWayPoints.Events
    if msg.HasField("msgBusWayPoints"):
        for wp in msg.msgBusWayPoints.Events:
            rows.append(_wp_to_dict(msg_type_value, wp))

    # (msgBusWayPointBatch hiện chỉ có vehicle/driver, không có GPS, nên tạm bỏ qua)

    return rows


# UDF trả về array<struct<...>>
decode_udf = udf(decode_busgps_from_b64, ArrayType(busgps_schema))


def main():
    print("Starting Spark job: Decode UFMS BusGPS from base64 parquet -> TABLE")

    spark = (
        SparkSession.builder
        .appName("DecodeUFMSBusGPS_Table_Cluster")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---------- STEP 1: Đọc parquet gốc ----------
    print(f" Reading input parquet from: {INPUT_FILE}")
    df_raw = spark.read.parquet(INPUT_FILE)

    print("\n Schema parquet gốc:")
    df_raw.printSchema()

    print("\n Sample 5 rows (raw parquet):")
    df_raw.show(5, truncate=False)

    print(f"\nUsing column '{COLUMN_NAME}' as base64 input")

    # ---------- STEP 2: Áp dụng UDF decode -> array<struct> ----------
    df_with_arr = df_raw.select(
        decode_udf(col(COLUMN_NAME)).alias("events")
    )

    # explode: mỗi BusWayPoint thành 1 dòng
    df_flat = df_with_arr.select(explode(col("events")).alias("event"))

    # lấy các cột của struct event
    df_bus = df_flat.select("event.*")

    print("\n Sample 5 decoded BusGPS TABLE rows:")
    df_bus.show(5, truncate=False)

    print("\n Count BusGPS rows:")
    count_bus = df_bus.count()
    print(f"BusGPS rows = {count_bus}")

    if count_bus == 0:
        print(" Không tìm thấy BusGPS nào, dừng.")
        return

    # ---------- STEP 3: Ghi CSV + Parquet ----------
    print(f"\n💾 Writing TABLE CSV to: {OUTPUT_CSV_DIR}")
    (
        df_bus
        .coalesce(1)  
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_CSV_DIR)
    )

    print(f"💾 Writing TABLE Parquet to: {OUTPUT_PARQUET_DIR}")
    df_bus.write.mode("overwrite").parquet(OUTPUT_PARQUET_DIR)

    print("\n✅ Đã hoàn tất pipeline.")


if __name__ == "__main__":
    main()
