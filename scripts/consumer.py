from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from pyspark.sql.functions import from_json, col, to_timestamp, when

# Khởi tạo SparkSession với Iceberg + Kafka + MinIO
spark = (
    SparkSession.builder 
    .appName("KafkaIcebergConsumer") 
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5",
            "org.apache.kafka:kafka-clients:3.4.1",
            "org.apache.kafka:kafka_2.12:3.4.1"
        ])
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") 
    .config("spark.sql.catalog.iceberg.type", "rest") 
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") 
    .config("spark.sql.catalog.iceberg.table-default.format-version", "2") 
    .config("spark.sql.defaultCatalog", "iceberg") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


KAFKA_BOOTSTRAP = "kafka-broker-1:29092"
TOPIC = "bus_gps_raw"
CATALOG_TABLE = "iceberg.demo.bus_gps_raw_v3"  # phải khớp với spark-defaults.conf

# Schema cho payload (khi producer gửi JSON per record)
schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("date", StringType(), True),
    StructField("vehicle", StringType(), True),
    StructField("lng", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("driver", StringType(), True),
    StructField("speed", StringType(), True),
    StructField("door_up", StringType(), True),
    StructField("door_down", StringType(), True)
])

# Tạo bảng nếu chưa có (schema Iceberg)
spark.sql(
    """
    CREATE NAMESPACE IF NOT EXISTS iceberg.demo
    """
)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_TABLE} (
    datetime TIMESTAMP,
    date STRING,
    vehicle STRING,
    lng DOUBLE,
    lat DOUBLE,
    driver STRING,
    speed DOUBLE,
    door_up BOOLEAN,
    door_down BOOLEAN
) USING iceberg
""")
print(f"✅ Using REST catalog table: {CATALOG_TABLE}")
print("Start to read stream from bus_gps_raw topic")

def write_batch_to_iceberg(batch_df, batch_id):
    # bỏ qua batch rỗng
    if batch_df.isEmpty():
        return

    rec_count = batch_df.count()
    print(f"[DEBUG] Batch {batch_id}: received {rec_count} records. Showing up to 10 rows:")
    batch_df.show(10, truncate=False)

    # convert kiểu dữ liệu; producer gửi CSV via JSON -> các cột ban đầu là string
    df = batch_df \
        .withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("lng", col("lng").cast("double")) \
        .withColumn("lat", col("lat").cast("double")) \
        .withColumn("speed", col("speed").cast("double")) \
        .withColumn("door_up", when(col("door_up").isin("True", "true", "1"), True).otherwise(False)) \
        .withColumn("door_down", when(col("door_down").isin("True", "true", "1"), True).otherwise(False)) \
        .select("datetime","date","vehicle","lng","lat","driver","speed","door_up","door_down")

    # ghi vào Iceberg (append)
    df.writeTo(CATALOG_TABLE).append()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# startingOffsets có nên đổi thành Latest hay không ?
# parse JSON payload (producer serializes dict -> JSON string)
value_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
parsed = value_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# start streaming với foreachBatch
checkpoint = "s3a://warehouse/checkpoints/bus_gps_raw_v3"  # hoặc path local /tmp/checkpoints/...
query = parsed.writeStream \
    .foreachBatch(write_batch_to_iceberg) \
    .option("checkpointLocation", checkpoint) \
    .trigger(processingTime="90 seconds") \
    .start()

query.awaitTermination()