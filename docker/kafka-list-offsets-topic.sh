#!/bin/bash

if [[ $# == 0 ]]; then
  echo "Usage: $0 <topic name>"
  echo "  e.g. $0 b1"
  echo "List offsets for all partitions of the given topic"
  exit 1
fi

topic=$1

broker="kafka-1"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"

docker exec -it "$broker" kafka-run-class kafka.tools.GetOffsetShell \
 --bootstrap-server "$brokers" --command-config /etc/kafka/secrets/admin-scram.properties \
 --topic "$topic"
