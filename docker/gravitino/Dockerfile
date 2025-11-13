FROM apache/gravitino:latest

# Tải JDBC driver
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/gravitino/libs/postgresql-42.7.4.jar

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-rest-server/1.9.2/iceberg-rest-server-1.9.2.jar /opt/gravitino/iceberg-rest-server/libs/iceberg-rest-server-1.9.2.jar

# Copy các file cấu hình nếu cần
COPY config/gravitino /opt/gravitino/conf