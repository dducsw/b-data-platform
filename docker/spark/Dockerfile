FROM openjdk:11-jre-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_VERSION=4.0.0
ENV HADOOP_VERSION=3.4
ENV DELTA_VERSION=4.0.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" $SPARK_HOME \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download Delta Lake jars
RUN wget -q "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
    -O $SPARK_HOME/jars/delta-spark_2.12-${DELTA_VERSION}.jar

RUN wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    -O $SPARK_HOME/jars/delta-storage-${DELTA_VERSION}.jar

# Download additional connectors
RUN wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/4.0.0-preview1/spark-sql-kafka-0-10_2.12-4.0.0-preview1.jar" \
    -O $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-4.0.0-preview1.jar

RUN wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar" \
    -O $SPARK_HOME/jars/hadoop-aws-3.4.0.jar

RUN wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar" \
    -O $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.367.jar

# Create spark user
RUN useradd -r -u 185 -g 0 spark && \
    mkdir -p $SPARK_HOME/work-dir && \
    chown -R spark:0 $SPARK_HOME && \
    chmod -R g+w $SPARK_HOME

# Copy entrypoint script
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

WORKDIR $SPARK_HOME

USER spark

EXPOSE 7077 8080 8081

ENTRYPOINT ["/opt/entrypoint.sh"]