#!/usr/bin/env python3
"""
Spark MinIO Direct Write Script
===============================
Direct script to write data to MinIO S3 without catalog
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime

print("🚀 Spark MinIO Direct Write Script")
print("=" * 40)

# Create Spark session with S3/MinIO configuration
print("⚡ Creating Spark session with MinIO configuration...")
spark = SparkSession.builder \
    .appName("SparkMinIODirectWrite") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

try:
    # Create sample data
    print("📊 Creating 3 sample BusGPS records...")
    data = [
        ("BUS_001", "ROUTE_1", 10.7769, 106.7009, 45.5, datetime(2024, 10, 29, 10, 30, 0)),
        ("BUS_002", "ROUTE_2", 10.7829, 106.6819, 38.2, datetime(2024, 10, 29, 10, 31, 0)),
        ("BUS_003", "ROUTE_1", 10.7909, 106.6919, 52.1, datetime(2024, 10, 29, 10, 32, 0))
    ]
    
    # Define schema
    schema = StructType([
        StructField("bus_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    print("✅ DataFrame created!")
    
    # Show data before writing
    print("\n📋 Data to be written to MinIO:")
    df.show(truncate=False)
    
    # Write to MinIO as Parquet format
    print("💾 Writing data to MinIO S3 bucket...")
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("path", "s3a://data-lake/bus-gps/year=2024/month=10/day=29/") \
        .parquet("s3a://data-lake/bus-gps/year=2024/month=10/day=29/")
    print("✅ Data written to MinIO!")
    
    # Read back from MinIO to verify
    print("🔍 Reading data back from MinIO...")
    read_df = spark.read.parquet("s3a://data-lake/bus-gps/year=2024/month=10/day=29/")
    print(f"📈 Records read: {read_df.count()}")
    
    print("\n📋 Data from MinIO:")
    read_df.show(truncate=False)
    
    # Write as JSON format too
    print("💾 Writing data as JSON format...")
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .json("s3a://data-lake/bus-gps-json/year=2024/month=10/day=29/")
    print("✅ JSON data written to MinIO!")
    
    # Simple query on the data
    print("\n🚍 Filtering buses on ROUTE_1:")
    read_df.createOrReplaceTempView("bus_gps")
    route1_df = spark.sql("""
        SELECT bus_id, speed, latitude, longitude, timestamp
        FROM bus_gps 
        WHERE route_id = 'ROUTE_1'
        ORDER BY timestamp
    """)
    route1_df.show()
    
    # Write filtered data to separate path
    print("💾 Writing ROUTE_1 data to separate path...")
    route1_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet("s3a://data-lake/bus-gps-route1/year=2024/month=10/day=29/")
    print("✅ ROUTE_1 data written!")
    
    # Show some statistics
    print("\n📊 Data Statistics:")
    print(f"Total buses: {read_df.select('bus_id').distinct().count()}")
    print(f"Total routes: {read_df.select('route_id').distinct().count()}")
    print(f"Average speed: {read_df.agg({'speed': 'avg'}).collect()[0][0]:.2f} km/h")
    
    print("\n🎉 Script completed successfully!")
    print("📁 Data locations in MinIO:")
    print("   - Parquet: s3a://data-lake/bus-gps/year=2024/month=10/day=29/")
    print("   - JSON: s3a://data-lake/bus-gps-json/year=2024/month=10/day=29/")
    print("   - Route1: s3a://data-lake/bus-gps-route1/year=2024/month=10/day=29/")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    raise
finally:
    spark.stop()
