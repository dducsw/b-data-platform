FROM apache/gravitino:latest

# Tải JDBC driver
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/gravitino/libs/postgresql-42.7.4.jar

# Copy các file cấu hình nếu cần
COPY config/gravitino /opt/gravitino/conf