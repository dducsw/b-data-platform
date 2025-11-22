1. Kiểm tra IP của Doris FE:
docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" doris-fe


Ví dụ:
172.18.0.2

Thay vào: 
doris-be:
    image: apache/doris:be-2.1.9
    container_name: doris-be
    ports:
      - "8040:8040"   # BE HTTP
      - "9050:9050"   # BE Internal
    depends_on:
      - doris-fe
    environment:
      FE_SERVERS: fe1:172.18.0.2:9010 # Thay IP ở đây
      BE_ADDR: 0.0.0.0:9050

2. Sử dụng MySQL Client (để kết nối Doris FE qua MySQL)
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root


[Hoặc sử dụng web UI: ](http://localhost:8030/Playground/result/bus_db-undefined)


3. Tạo database trong doris tên là bus_db và
   
   CREATE TABLE bus_gps (
    `datetime` DATETIME,
    `vehicle` VARCHAR(20),
    `date` DATE,
    `lng` DOUBLE,
    `lat` DOUBLE,
    `driver` VARCHAR(50),
    `speed` DOUBLE,
    `door_up` BOOLEAN,
    `door_down` BOOLEAN
)
DUPLICATE KEY(`datetime`, `vehicle`)
DISTRIBUTED BY HASH(`vehicle`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

4. Tạo rountine


CREATE ROUTINE LOAD bus_gps_job ON bus_gps
COLUMNS(datetime, vehicle, date, lng, lat, driver, speed, door_up, door_down)
PROPERTIES
(
    "format" = "json",
    "max_batch_interval" = "10",
    "max_batch_rows" = "100000"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka-broker-1:29092",
    "kafka_topic" = "bus_gps_raw",
    "property.group.id" = "doris_gps_consumer_group",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

5. Kiểm tra routine
   
SHOW ROUTINE LOAD FOR bus_gps_job;



