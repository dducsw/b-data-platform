FROM openjdk:17-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3 and pip
RUN apt-get update && apt-get install -y python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*



# Set environment variables
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3
ENV ICEBERG_VERSION=1.9.2
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_JARS_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"


# Install Python packages for Iceberg
RUN pip3 install pyiceberg[s3fs,pyarrow]
# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" $SPARK_HOME \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download Iceberg jars
RUN wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    -O $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

# Download Iceberg AWS integration
RUN wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar" \
    -O $SPARK_HOME/jars/iceberg-aws-${ICEBERG_VERSION}.jar

# Download AWS SDK for S3 support
RUN wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar" \
    -O $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.565.jar

# Download Hadoop AWS
RUN wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    -O $SPARK_HOME/jars/hadoop-aws-3.3.4.jar

# Download additional connectors
RUN wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar" \
    -O $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar



RUN wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    -O $SPARK_HOME/jars/hadoop-aws-3.3.4.jar

# Create spark user
RUN useradd -r -u 185 -g 0 spark && \
    mkdir -p $SPARK_HOME/work-dir && \
    chown -R spark:0 $SPARK_HOME && \
    chmod -R g+w $SPARK_HOME

# Copy entrypoint script
COPY entrypoint.sh /opt/entrypoint.sh

# Fix quyền + line ending
RUN apt-get update && apt-get install -y dos2unix && \
    dos2unix /opt/entrypoint.sh && \
    chmod +x /opt/entrypoint.sh && \
    chown spark:0 /opt/entrypoint.sh

WORKDIR $SPARK_HOME

RUN mkdir -p $SPARK_HOME/logs && \
    chown -R spark:0 $SPARK_HOME/logs && \
    chmod -R 775 $SPARK_HOME/logs
    
USER spark

EXPOSE 7077 8080 8081

ENTRYPOINT ["/opt/entrypoint.sh"]
