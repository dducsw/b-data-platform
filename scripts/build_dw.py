from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    unbase64,
    from_unixtime,
    to_timestamp,
    to_date,
    when,
    lit,
    monotonically_increasing_id,
)
from pyspark.sql.protobuf.functions import from_protobuf
# docker exec -it spark-master wget "https://repo1.maven.org/maven2/org/apache/spark/spark-protobuf_2.12/3.5.5/spark-protobuf_2.12-3.5.5.jar" -O /opt/spark/jars/spark-protobuf_2.12-3.5.5.jar
# docker exec -it spark-worker-1 wget "https://repo1.maven.org/maven2/org/apache/spark/spark-protobuf_2.12/3.5.5/spark-protobuf_2.12-3.5.5.jar" -O /opt/spark/jars/spark-protobuf_2.12-3.5.5.jar
# docker exec -it spark-worker-2 wget "https://repo1.maven.org/maven2/org/apache/spark/spark-protobuf_2.12/3.5.5/spark-protobuf_2.12-3.5.5.jar" -O /opt/spark/jars/spark-protobuf_2.12-3.5.5.jar
# docker-compose exec spark-master spark-submit --master spark://spark-master:7077 scripts/build_dw.py

INPUT_PARQUET_PATH = "data/base64_10M.parquet"   
UFMS_DESC_PATH = "data/ufms.desc"
TARGET_DB = "dw"

MINIO_BUCKET = "warehouse"  
MINIO_BASE_PATH = f"s3a://{MINIO_BUCKET}/dwhouse"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
# ==========================

def decode_and_split_2(spark: SparkSession):
    print("Bắt đầu decode")
    df = spark.read.parquet(INPUT_PARQUET_PATH)

    print("Schema parquet gốc:")
    df.printSchema()
    if "raw_base64" not in df.columns:
        raise ValueError("Không tìm thấy cột 'raw_base64' trong parquet!")
    df = df.withColumn("data_bytes", unbase64("raw_base64"))
    decoded_df = df.withColumn(
        "decoded",
        from_protobuf(
            col("data_bytes"),
            messageName="UFMS.BaseMessage",
            descFilePath=UFMS_DESC_PATH,
        ),
    )

    print("[INFO] msgType phân biệt trong decoded_df:")
    decoded_df.select("decoded.msgType").distinct().show(truncate=False)

    df_waypoint = decoded_df.filter(
        col("decoded.msgType") == "MsgType_WayPoint"
    ).select("decoded.msgWayPoint.*")

    df_buswaypoint = decoded_df.filter(
        col("decoded.msgType") == "MsgType_BusWayPoint"
    ).select("decoded.msgBusWayPoint.*")

    return df_waypoint, df_buswaypoint
# BUILD DIM
def build_dim_waypoint(df_waypoint):
    print("Build dim_waypoint ...")
    if df_waypoint.rdd.isEmpty():
        print("[WARN] df_waypoint rỗng, dim_waypoint sẽ không có dữ liệu.")

    dim = (
        df_waypoint
        .select(
            "vehicle",
            "driver",
            col("x").alias("lng"),
            col("y").alias("lat"),
            "location",
            "heading",
            "ignition",
            "door",
            "aircon",
            "maxvalidspeed",
            "vss",
        )
        .dropDuplicates()
        .withColumn("waypoint_sk", monotonically_increasing_id())
    )

    dim = dim.select(
        "waypoint_sk",
        "vehicle",
        "driver",
        "lng",
        "lat",
        "location",
        "heading",
        "ignition",
        "door",
        "aircon",
        "maxvalidspeed",
        "vss",
    )
    return dim


def build_dim_buswaypoint(df_buswaypoint):
    print("=" * 80)
    print("[INFO] Build dim_buswaypoint ...")
    if df_buswaypoint.rdd.isEmpty():
        print("[WARN] df_buswaypoint rỗng, dim_buswaypoint sẽ không có dữ liệu.")

    dim = (
        df_buswaypoint
        .select(
            "vehicle",
            "driver",
            "heading",
            "ignition",
            "aircon",
            "sos",
            "working",
            "analog1",
            "analog2",
        )
        .dropDuplicates()
        .withColumn("bus_waypoint_sk", monotonically_increasing_id())
    )

    dim = dim.select(
        "bus_waypoint_sk",
        "vehicle",
        "driver",
        "heading",
        "ignition",
        "aircon",
        "sos",
        "working",
        "analog1",
        "analog2",
    )
    return dim
# BUILD FACT

def build_fact_bus_waypoint(df_buswaypoint):
    """
    fact_bus_waypoint với schema:
      datetime, date, vehicle, lng, lat, driver, speed, door_up, door_down
    """
    if df_buswaypoint.rdd.isEmpty():
        print("df_buswaypoint rỗng, fact_bus_waypoint sẽ không có dữ liệu.")

    fact = (
        df_buswaypoint
        .withColumn("event_ts", to_timestamp(from_unixtime(col("datetime"))))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("lng", col("x"))
        .withColumn("lat", col("y"))
        .withColumn("door_up_int", when(col("door_up") == True, lit(1)).otherwise(lit(0)))
        .withColumn("door_down_int", when(col("door_down") == True, lit(1)).otherwise(lit(0)))
    )

    fact = fact.select(
        col("event_ts").alias("datetime"),
        col("event_date").alias("date"),
        col("vehicle"),
        col("lng"),
        col("lat"),
        col("driver"),
        col("speed"),
        col("door_up_int").alias("door_up"),
        col("door_down_int").alias("door_down"),
    )

    return fact

# 4. SAVE EXTERNAL TABLE LÊN MINIO

def save_table_and_minio(df, spark: SparkSession, table_name: str):
    full_name = f"{TARGET_DB}.{table_name}"
    minio_path = f"{MINIO_BASE_PATH}/{table_name}/"

    print("=" * 80)
    print(f"Ghi bảng EXTERNAL + Parquet lên MinIO cho: {full_name}")
    print(f"MinIO path: {minio_path}")

    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("path", minio_path)
        .saveAsTable(full_name)
    )

    print(f"Schema của {full_name}:")
    spark.table(full_name).printSchema()
    print(f" 5 dòng đầu của {full_name}:")
    spark.table(full_name).show(5, truncate=False)
# MAIN

def main():
    spark = (
        SparkSession.builder
        .appName("BuildBusGPSDWH")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")

    print("=== BẮT ĐẦU BUILD DATA WAREHOUSE===")

    df_waypoint, df_buswaypoint = decode_and_split_2(spark)
    dim_waypoint = build_dim_waypoint(df_waypoint)
    dim_buswaypoint = build_dim_buswaypoint(df_buswaypoint)
    #Build fact
    fact_bus_waypoint = build_fact_bus_waypoint(df_buswaypoint)
    save_table_and_minio(dim_waypoint, spark, "dim_waypoint")
    save_table_and_minio(dim_buswaypoint, spark, "dim_buswaypoint")
    save_table_and_minio(fact_bus_waypoint, spark, "fact_bus_waypoint")

    print("=== HOÀN THÀNH BUILD DWH + PUSH MINIO ===")


if __name__ == "__main__":
    main()
