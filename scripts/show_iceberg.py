from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ShowIceberg")
    .getOrCreate()    # dùng spark-defaults.conf
)
spark.sparkContext.setLogLevel("ERROR")

print("\n=== Namespaces trong catalog iceberg ===")
spark.sql("SHOW NAMESPACES IN iceberg").show(truncate=False)

print("\n=== Tables trong namespace demo (nếu có) ===")
spark.sql("SHOW TABLES IN iceberg.demo").show(truncate=False)


spark.stop()
