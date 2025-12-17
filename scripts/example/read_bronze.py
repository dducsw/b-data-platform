from pyspark.sql import SparkSession
import traceback

def read_bronze_tables():
    # Initialize SparkSession with Iceberg
    spark = SparkSession.builder \
        .appName("ReadBronzeTables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lake/") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .   config("spark.sql.catalog.iceberg.cache-enabled", "false") \
        .config("spark.cores.max", "2") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Namespace for bronze tables
    namespace = "iceberg.bronze"

    # List of tables to check
    tables = [
        "waypoint",
        "buswaypoint",
        "studentcheckin",
        "regvehicle",
        "regdriver",
        "regcompany",
        "busticketpoint",
        "busticketstartend",
        "buswaypointbatch"
    ]

    print("\nChecking tables in namespace: bronze\n")
    spark.sql("SHOW TABLES IN iceberg.bronze").show()

    for table in tables:
        table_name = f"{namespace}.{table}"
        try:
            # Refresh table
            spark.sql(f"REFRESH TABLE {table_name}")
            
            # Read the table
            df = spark.read.format("iceberg").load(table_name)
            print("Read", table_name)
            
            
            
            spark.sql(f"DESCRIBE TABLE {table_name}").show()

            # Debugging: Print schema
            df.printSchema()
            
            # Debugging: Check row count
            try:
                row_count = df.count()
                print(f"Row count for {table_name}: {row_count}")
            except Exception as count_error:
                print(f"Error during row count for {table_name}: {count_error}")

            # Check if the table has data
            if row_count > 0:
                print(f"\nData found in table: {table_name}")
                df.show(10, truncate=False)
            else:
                print(f"No data in table: {table_name}")
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            traceback.print_exc()

    spark.stop()

if __name__ == "__main__":
    read_bronze_tables()
