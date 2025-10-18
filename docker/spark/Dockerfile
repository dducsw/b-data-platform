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
ENV DELTA_VERSION=3.2.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_JARS_PACKAGES="io.delta:delta-spark_2.13:3.2.0"


# Install delta-spark Python package
RUN pip3 install delta-spark==${DELTA_VERSION}
# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" $SPARK_HOME \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download Delta Lake jars (for Scala 2.13)
RUN wget -q "https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" \
    -O $SPARK_HOME/jars/delta-spark_2.13-${DELTA_VERSION}.jar

RUN wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    -O $SPARK_HOME/jars/delta-storage-${DELTA_VERSION}.jar

# Download additional connectors
RUN wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.1/spark-sql-kafka-0-10_2.13-3.5.1.jar" \
    -O $SPARK_HOME/jars/spark-sql-kafka-0-10_2.13-3.5.1.jar



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
