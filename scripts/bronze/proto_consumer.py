from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, current_timestamp
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.storagelevel import StorageLevel

# ==========================================
# 1. CẤU HÌNH KẾT NỐI (DORIS & KAFKA)
# ==========================================
KAFKA_BOOTSTRAP = "kafka-broker-1:29092"
TOPIC = "bus_gps_protobuf_final"

# Cấu hình Doris
DORIS_FE_NODES = "doris-fe:8030"  # Port HTTP của Doris FE
DORIS_USER = "root"
DORIS_PASSWORD = ""
DORIS_DB = "demo_data"

# Mapping: Loại tin nhắn -> Tên bảng Doris
# (Đảm bảo bạn đã tạo các bảng này trong Doris bằng SQL trước)
doris_tables = {
    "MsgType_BusWayPoint": f"{DORIS_DB}.bus_waypoint",
    "MsgType_WayPoint": f"{DORIS_DB}.waypoint",
    # Thêm các bảng khác nếu cần thiết, ví dụ:
    # "MsgType_StudentCheckInPoint": f"{DORIS_DB}.student_checkin",
}

# Khởi tạo SparkSession với Iceberg + Kafka + MinIO
spark = SparkSession.builder \
    .appName("ProtoKafkaIcebergConsumer") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://lake/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.cache-enabled", "false") \
    .config("spark.cores.max", "2") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

KAFKA_BOOTSTRAP = "kafka-broker-1:29092"
TOPIC = "bus_gps_protobuf_final"

# Decode protobuf payload
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS BINARY) AS raw_bytes")
decoded_df = value_df.withColumn(
    "decoded",
    from_protobuf(
        col("raw_bytes"),
        messageName="UFMS.BaseMessage",
        descFilePath="data/ufms.desc"
    )
).select("decoded.*")
decoded_df.printSchema()


# Phân loại theo msgType và ghi vào các bảng Iceberg
bronze_tables = {
    "MsgType_BusWayPoint": "iceberg.bronze.buswaypoint",
    "MsgType_WayPoint": "iceberg.bronze.waypoint",
    "MsgType_StudentCheckInPoint": "iceberg.bronze.studentcheckin",
    "MsgType_RegVehicle": "iceberg.bronze.regvehicle",
    "MsgType_RegDriver": "iceberg.bronze.regdriver",
    "MsgType_RegCompany": "iceberg.bronze.regcompany",
    "MsgType_BusTicketPoint": "iceberg.bronze.busticketpoint",
    "MsgType_BusTicketStartEndPoint": "iceberg.bronze.busticketstartend",
    "MsgType_BusWayPointBatch": "iceberg.bronze.buswaypointbatch"
}
def save_to_doris(df, table_identifier):
    try:
        # Nếu bảng Doris không có cột load_at thì bỏ dòng này đi
        # df_clean = df.drop("load_at") 
        
        print(f"--> [DORIS] Writing to table: {table_identifier}")
        df.write \
            .format("doris") \
            .option("doris.table.identifier", table_identifier) \
            .option("doris.fenodes", DORIS_FE_NODES) \
            .option("doris.user", DORIS_USER) \
            .option("doris.password", DORIS_PASSWORD) \
            .option("doris.sink.batch.size", "1000") \
            .option("doris.sink.max-retries", "3") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"❌ [DORIS ERROR] Failed to write {table_identifier}: {e}")

def write_batch_to_iceberg(batch_df, batch_id):
    print("Start write")
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    
    try:
        if batch_df.isEmpty():
            return

        rec_count = batch_df.count()
        print(f"[DEBUG] Batch {batch_id}: received {rec_count} records. Showing up to 10 rows:")
        batch_df.show(10, truncate=False)

        # Correctly filter and select fields based on message type enums
        df_buswaypoint = batch_df.filter(col("msgType") == "MsgType_BusWayPoint") \
            .select("msgBusWayPoint.*") \
            .withColumn("datetime", from_unixtime(col("datetime")).cast("timestamp"))  # Explicitly cast to TIMESTAMP

        df_waypoint = batch_df.filter(col("msgType") == "MsgType_WayPoint") \
            .select("msgWayPoint.*") \
            .withColumn("datetime", from_unixtime(col("datetime")).cast("timestamp"))  # Explicitly cast to TIMESTAMP

        df_buswaypoints = batch_df.filter(col("msgType") == "MsgType_BusWayPoints") \
            .selectExpr("explode(msgBusWayPoints.Events) as event") \
            .select("event.*") \
            .withColumn("datetime", from_unixtime(col("datetime")).cast("timestamp"))  # Explicitly cast to TIMESTAMP

        df_buswaypointbatch = batch_df.filter(col("msgType") == "MsgType_BusWayPointBatch") \
            .select("msgBusWayPointBatch.*")

        df_studentcheckin = batch_df.filter(col("msgType") == "MsgType_StudentCheckInPoint") \
            .select("msgStudentCheckInPoint.*")

        df_regvehicle = batch_df.filter(col("msgType") == "MsgType_RegVehicle") \
            .select("msgRegVehicle.*")

        df_regdriver = batch_df.filter(col("msgType") == "MsgType_RegDriver") \
            .select("msgRegDriver.*")

        df_regcompany = batch_df.filter(col("msgType") == "MsgType_RegCompany") \
            .select("msgRegCompany.*")

        df_busticketpoint = batch_df.filter(col("msgType") == "MsgType_BusTicketPoint") \
            .select("msgBusTicketPoint.*")

        df_busticketstartend = batch_df.filter(col("msgType") == "MsgType_BusTicketStartEndPoint") \
            .select("msgBusTicketStartEndPoint.*")

        # Write each filtered DataFrame to its corresponding Iceberg table with commit options
        if not df_buswaypoint.isEmpty():
            df_buswaypoint \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.buswaypoint") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_waypoint.isEmpty():
            df_waypoint \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.waypoint") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_buswaypoints.isEmpty():
            df_buswaypoints \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.buswaypoint") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_buswaypointbatch.isEmpty():
            df_buswaypointbatch \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.buswaypointbatch") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_studentcheckin.isEmpty():
            df_studentcheckin \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.studentcheckin") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_regvehicle.isEmpty():
            df_regvehicle \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.regvehicle") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_regdriver.isEmpty():
            df_regdriver \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.regdriver") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_regcompany.isEmpty():
            df_regcompany \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.regcompany") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_busticketpoint.isEmpty():
            df_busticketpoint \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.busticketpoint") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
        if not df_busticketstartend.isEmpty():
            df_busticketstartend \
                .withColumn("load_at", current_timestamp()) \
                .writeTo("iceberg.bronze.busticketstartend") \
                .option("commit.manifest.min-count-to-merge", "1") \
                .option("write.metadata.metrics.default", "full") \
                .append()
                
    finally:
        batch_df.unpersist()

def write_batch_to_iceberg_and_doris(batch_df, batch_id):
    print(f"Start processing batch {batch_id}")
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    
    try:
        if batch_df.isEmpty():
            return

        rec_count = batch_df.count()
        print(f"[DEBUG] Batch {batch_id}: received {rec_count} records.")
        # batch_df.show(5) # Tắt show để đỡ tốn thời gian

        # --- Helper function xử lý từng loại tin nhắn ---
        def process_msg_type(df_source, msg_type, iceberg_table, doris_table=None):
            # 1. Lọc dữ liệu
            filtered = df_source.filter(col("msgType") == msg_type)
            if filtered.isEmpty():
                return

            # 2. Làm sạch & Chọn cột (Logic cũ của bạn)
            if msg_type == "MsgType_BusWayPoint":
                final_df = filtered.select("msgBusWayPoint.*") \
                    .withColumn("datetime", from_unixtime(col("datetime")).cast("timestamp"))
            elif msg_type == "MsgType_WayPoint":
                final_df = filtered.select("msgWayPoint.*") \
                    .withColumn("datetime", from_unixtime(col("datetime")).cast("timestamp"))
            # ... Bạn có thể thêm các elif cho các loại tin khác nếu cần ...
            else:
                 # Mặc định cho các loại chưa định nghĩa rõ
                 return 

            # Tạo cột load_at cho Iceberg/Doris
            df_ready = final_df.withColumn("load_at", current_timestamp())

            # =========================================================
            # [TEST MODE] TẠM THỜI TẮT GHI ICEBERG ĐỂ KIỂM TRA DORIS
            # =========================================================
            # print(f"   -> Writing Iceberg: {iceberg_table}")
            # df_ready.writeTo(iceberg_table).append()
            print(f"   [SKIP] Tạm bỏ qua Iceberg {iceberg_table} để test Doris.")

            # =========================================================
            # [DEBUG] GHI VÀO DORIS
            # =========================================================
            if doris_table:
                print(f"👉 [CHECKPOINT] Chuẩn bị ghi vào Doris bảng: {doris_table}")
                try:
                    # Gọi hàm save_to_doris (Đảm bảo hàm này đã được define ở trên)
                    save_to_doris(df_ready, doris_table)
                    print(f"✅ [SUCCESS] Đã ghi thành công vào Doris: {doris_table}")
                except Exception as e:
                    print(f"❌ [ERROR] LỖI KHI GHI DORIS: {e}")
            else:
                print(f"⚠️ [WARN] Không tìm thấy cấu hình Doris cho loại tin: {msg_type}")

        # --- GỌI HÀM XỬ LÝ (Chỉ test loại quan trọng nhất trước) ---
        
        # 1. BusWayPoint
        process_msg_type(batch_df, "MsgType_BusWayPoint", 
                         bronze_tables["MsgType_BusWayPoint"], 
                         doris_tables.get("MsgType_BusWayPoint"))

        # 2. WayPoint
        process_msg_type(batch_df, "MsgType_WayPoint", 
                         bronze_tables["MsgType_WayPoint"], 
                         doris_tables.get("MsgType_WayPoint"))

    except Exception as e:
        print(f"CRITICAL ERROR in batch {batch_id}: {e}")
    finally:
        batch_df.unpersist()
print("Starting streaming query...")
query = decoded_df.writeStream \
    .foreachBatch(write_batch_to_iceberg) \
    .option("checkpointLocation", "s3a://lake/checkpoints/bus_gps_protobuf_final") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()