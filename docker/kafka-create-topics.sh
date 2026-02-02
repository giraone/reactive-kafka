#!/bin/bash

broker="kafka-1"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"
replicationFactor=3
secretFile=/etc/kafka/secrets/admin-scram.properties

for topic in a1 b1
do
  echo "Create $topic"
  docker exec "$broker" kafka-topics \
      --bootstrap-server "$brokers" \
      --command-config "$secretFile" \
      --create --if-not-exists \
      --topic $topic \
      --replication-factor $replicationFactor \
      --partitions 8
done

