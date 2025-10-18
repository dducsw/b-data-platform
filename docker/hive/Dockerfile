FROM openjdk:8-jre-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV HIVE_VERSION=3.1.3
ENV HADOOP_VERSION=3.3.4
ENV HIVE_HOME=/opt/hive
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HIVE_HOME/bin:$HADOOP_HOME/bin

# Download and install Hadoop
RUN wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    && tar -xzf "hadoop-${HADOOP_VERSION}.tar.gz" \
    && mv "hadoop-${HADOOP_VERSION}" $HADOOP_HOME \
    && rm "hadoop-${HADOOP_VERSION}.tar.gz"

# Download and install Hive
RUN wget -q "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
    && tar -xzf "apache-hive-${HIVE_VERSION}-bin.tar.gz" \
    && mv "apache-hive-${HIVE_VERSION}-bin" $HIVE_HOME \
    && rm "apache-hive-${HIVE_VERSION}-bin.tar.gz"

# Download PostgreSQL JDBC driver
RUN wget -q "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar" \
    -O $HIVE_HOME/lib/postgresql-42.6.0.jar

# Copy configuration files
COPY hive-site.xml $HIVE_HOME/conf/hive-site.xml
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# Initialize schema

EXPOSE 9083

ENTRYPOINT ["/opt/entrypoint.sh"]