#!/bin/bash

set -e

echo "Starting Kafka Consumer: $CONSUMER_GROUP"

# Wait for Kafka brokers to be ready
until kafka-topics.sh --bootstrap-server $KAFKA_BROKERS --list > /dev/null 2>&1; do
  echo "Waiting for Kafka brokers..."
  sleep 5
done

echo "Kafka brokers are ready"

# Create topic if it doesn't exist
kafka-topics.sh --bootstrap-server $KAFKA_BROKERS \
  --create --if-not-exists \
  --topic $TOPIC_NAME \
  --partitions 3 \
  --replication-factor 2

echo "Starting consumer for topic: $TOPIC_NAME with group: $CONSUMER_GROUP"

# Start consumer
exec kafka-console-consumer.sh \
  --bootstrap-server $KAFKA_BROKERS \
  --topic $TOPIC_NAME \
  --group $CONSUMER_GROUP \
  --from-beginning