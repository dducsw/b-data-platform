FROM openjdk:11-jre-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV KAFKA_VERSION=3.9.0
ENV SCALA_VERSION=2.13

# Download Kafka
RUN wget -q "https://archive.apache.org/dist/kafka/2.13-${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    && tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    && mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka \
    && rm "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

ENV PATH=$PATH:/opt/kafka/bin

# Copy consumer script
COPY consumer.sh /opt/consumer.sh
RUN chmod +x /opt/consumer.sh

WORKDIR /opt/kafka

ENTRYPOINT ["/opt/consumer.sh"]