from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session for streaming"""
    return SparkSession.builder \
        .appName("KafkaStreamingExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Define schema for incoming JSON data
        schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("data", MapType(StringType(), StringType()), True)
        ])
        
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka-broker-1:29092,kafka-broker-2:29094") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = kafka_df \
            .select(
                col("key").cast("string").alias("key"),
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ) \
            .select("key", "data.*", "kafka_timestamp")
        
        # Add processing timestamp
        enriched_df = parsed_df \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("event_datetime", from_unixtime(col("timestamp")))
        
        # Write to Delta Lake (batch mode for demo)
        print("Starting stream processing...")
        
        query = enriched_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "s3a://datalake/checkpoints/user-events") \
            .option("path", "s3a://datalake/delta-tables/user-events") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # Also write to console for monitoring
        console_query = enriched_df \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print("Stream processing started. Press Ctrl+C to stop...")
        
        # Wait for termination
        query.awaitTermination()
        console_query.awaitTermination()
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()