from pyspark.sql import SparkSession

# Khởi tạo SparkSession với Iceberg
spark = SparkSession.builder \
    .appName("BusAnalyticsSparkSQL") \
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

# docker-compose exec spark-master spark-submit --master spark://spark-master:7077 scripts/silver/highlevelquery.py

spark.sql("""
CREATE OR REPLACE TEMP VIEW bus AS 
SELECT * FROM iceberg.silver.buswaypoint      
""")

# Hoạt động trong ngày
df_overview = spark.sql("""
    WITH ordered AS (
        SELECT
            vehicle,
            datetime,
            ignition,
            LAG(datetime) OVER (
                PARTITION BY vehicle
                ORDER BY datetime
            ) AS prev_time,
            LAG(ignition) OVER (
                PARTITION BY vehicle
                ORDER BY datetime
            ) AS prev_ignition
        FROM iceberg.silver.buswaypoint
    )
    SELECT
        vehicle,
        ROUND(
            SUM(
                CASE
                    WHEN prev_ignition = true
                    AND ignition = true
                    THEN unix_timestamp(datetime) - unix_timestamp(prev_time)
                    ELSE 0
                END
            ) / 60, 2
        ) AS active_minutes
    FROM ordered
    GROUP BY vehicle;
""")
print("========== Bus Active Minutes Overview: ===============")
df_overview.show(truncate=False)
# Đếm số lượng chuyến xe
df_trip = spark.sql("""
WITH ordered AS (
    SELECT vehicle, ignition, LAG(ignition) OVER (
        PARTITION BY vehicle
        ORDER BY datetime
    ) AS prev_ignition
    FROM iceberg.silver.buswaypoint
)
SELECT vehicle, COUNT(*) AS trip_point
FROM ordered
WHERE prev_ignition = false AND ignition = true
GROUP BY vehicle
""")
print ("========== Bus Trip Count Overview: ===============")
df_trip.show(truncate=False)
# Thời lượng trung bình mỗi chuyến
df_avg_trip_duration = spark.sql("""
WITH ordered AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN ignition = true
                 AND LAG(ignition) OVER (PARTITION BY vehicle ORDER BY datetime) = false
                THEN 1 ELSE 0
            END
        ) OVER (PARTITION BY vehicle ORDER BY datetime) AS trip_id
    FROM iceberg.silver.buswaypoint
), 
trip_durations AS (
    SELECT
        vehicle,
        trip_id,
        (unix_timestamp(MAX(datetime)) - unix_timestamp(MIN(datetime))) AS time_duration
    FROM ordered
    WHERE ignition = true
    GROUP BY vehicle, trip_id
)
SELECT vehicle, ROUND(AVG(time_duration)/60, 2) as avg_time_duration
FROM trip_durations
GROUP BY vehicle
""")

print ("========== Bus Average Trip Duration Overview: ===============")
df_avg_trip_duration.show(truncate=False)
# Tốc độ trung bình
df_avg_speed = spark.sql("""
SELECT
    vehicle,
    ROUND(AVG(speed), 2) AS avg_speed
FROM iceberg.silver.buswaypoint
WHERE ignition = true
  AND speed >= 2
GROUP BY vehicle
""")

print("========== Bus Average Speed Overview: ===============")
df_avg_speed.show(truncate=False)

# Thời gian xe đứng yên 
df_idle_time = spark.sql("""
WITH ordered AS (
    SELECT
        vehicle,
        datetime,
        speed,
        ignition,
        LAG(datetime) OVER (
            PARTITION BY vehicle
            ORDER BY datetime
        ) AS prev_time
    FROM iceberg.silver.buswaypoint
)
SELECT
    vehicle,
    ROUND(
        SUM(
            CASE
                WHEN ignition = true AND speed < 3
                THEN unix_timestamp(datetime) - unix_timestamp(prev_time)
                ELSE 0
            END
        ) / 60, 2
    ) AS idle_minutes
FROM ordered
GROUP BY vehicle
""")
print("========== Bus Idle Time Overview: ===============")
df_idle_time.show(truncate=False)