#!/usr/bin/env python3
"""
Simple Apache Iceberg Script
============================
Direct script to create table and insert 3 sample records
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col, to_timestamp, when

print("🚀 Simple Apache Iceberg Script")
print("=" * 40)

# Create Spark session
print("⚡ Creating Spark session...")
spark = (
    SparkSession.builder 
    .appName("SimpleIcebergScript") 
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") 
    .config("spark.sql.catalog.iceberg.type", "rest") 
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.table-default.format-version", "2") 
    .config("spark.sql.defaultCatalog", "iceberg") 
    .getOrCreate()
)    
spark.sparkContext.setLogLevel("ERROR")
try:
    # Create namespace
    print("📋 Creating namespace...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
    
    # Create table with format version 2
    print("📋 Creating BusGPS table with Iceberg v2 format...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.demo.bus_gps (
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
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """)
    print("✅ Table created!")
    
    # Create sample data
    print("📊 Creating records...")
    # Define schema
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

    raw = (
        spark.read
            .option("header", True)
            .csv("data/raw_2025-04-01.csv")
    )
    # Create DataFrame
    df = (
        raw
        .withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("lng", col("lng").cast("double"))
        .withColumn("lat", col("lat").cast("double"))
        .withColumn("speed", col("speed").cast("double"))
        .withColumn("door_up",  when(col("door_up").isin("True", "true", "1"), True).otherwise(False))
        .withColumn("door_down", when(col("door_down").isin("True", "true", "1"), True).otherwise(False))
        .select("datetime","date","vehicle","lng","lat","driver","speed","door_up","door_down")
    )    
    # Write to Iceberg
    print("💾 Writing data to Iceberg table...")
    df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.demo.bus_gps")
    print("✅ Data written!")

    # 4. Read back
    print("🔍 Reading data from table...")
    result_df = spark.table("iceberg.demo.bus_gps")
    print(f"📈 Total records: {result_df.count()}")

    print("\n📋 Sample rows from BusGPS:")
    result_df.show(10, truncate=False)

    # 5. Simple query với đúng cột đang có
    print("\n🚍 Sample vehicles and positions:")
    spark.sql("""
        SELECT vehicle, speed, lng, lat, datetime
        FROM iceberg.demo.bus_gps
        ORDER BY datetime
        LIMIT 10
    """).show(truncate=False)

    print("\n🎉 Script completed successfully!")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    raise
finally:
    spark.stop()
