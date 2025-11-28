"""
Bronze Layer Table Definitions for UFMS
=======================================
Tạo các bảng Iceberg cho từng message chính trong UFMS.proto
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeTableDefinitions") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.table-default.format-version", "2") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

namespace = "bronze.ufms"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

tables = [
    # WayPoint
    ("waypoint", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.waypoint (
            vehicle STRING,
            driver STRING,
            speed FLOAT,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            z FLOAT,
            heading FLOAT,
            ignition BOOLEAN,
            door BOOLEAN,
            aircon BOOLEAN,
            maxvalidspeed DOUBLE,
            vss FLOAT,
            location STRING,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # BusWayPoint
    ("buswaypoint", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.buswaypoint (
            vehicle STRING,
            driver STRING,
            speed FLOAT,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            z FLOAT,
            heading FLOAT,
            ignition BOOLEAN,
            aircon BOOLEAN,
            door_up BOOLEAN,
            door_down BOOLEAN,
            sos BOOLEAN,
            working BOOLEAN,
            analog1 FLOAT,
            analog2 FLOAT,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # StudentCheckInPoint
    ("studentcheckin", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.studentcheckin (
            vehicle STRING,
            studentcode STRING,
            speed FLOAT,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            z FLOAT,
            heading FLOAT,
            working BOOLEAN,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # RegVehicle
    ("regvehicle", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.regvehicle (
            vehicle STRING,
            vehicleType INT,
            driver STRING,
            company STRING,
            deviceModelNo INT,
            deviceModel STRING,
            deviceId STRING,
            sim STRING,
            datetime TIMESTAMP,
            vin STRING,
            load_at TIMESTAMP
        ) USING iceberg
    """),
    # RegDriver
    ("regdriver", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.regdriver (
            driver STRING,
            name STRING,
            datetimeIssue TIMESTAMP,
            datetimeExpire TIMESTAMP,
            regPlace STRING,
            license STRING,
            load_at TIMESTAMP
        ) USING iceberg
    """),
    # RegCompany
    ("regcompany", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.regcompany (
            company STRING,
            name STRING,
            address STRING,
            tel STRING,
            load_at TIMESTAMP
        ) USING iceberg
    """),
    # BusTicketPoint
    ("busticketpoint", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.busticketpoint (
            vehicle STRING,
            routecode STRING,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            stationname STRING,
            tickettype INT,
            fare DOUBLE,
            series STRING,
            customercode STRING,
            universitycode STRING,
            companycode STRING,
            iscash BOOLEAN,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # BusTicketStartEndPoint
    ("busticketstartend", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.busticketstartend (
            vehicle STRING,
            routecode STRING,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            stationname STRING,
            tripno INT,
            pointtype INT,
            ticket_total INT,
            ticket_series STRING,
            ticket_type INT,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # BusWayPointBatch
    ("buswaypointbatch", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.ufms.buswaypointbatch (
            vehicle STRING,
            driver STRING,
            load_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
]

for name, stmt in tables:
    print(f"- Creating table: {name}")
    spark.sql(stmt)
    print(f"* Table {name} created!")

print("\n# Bronze layer tables created successfully!")

spark.stop()