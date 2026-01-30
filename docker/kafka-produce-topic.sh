#!/bin/bash

if [[ $# -lt 1 ]] ; then
    echo "Usage: $0 <topic>"
    echo " e.g.: $0 a1"
    exit 1
fi

broker="kafka-1"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"

docker exec -it ${broker} bash -c "kafka-console-producer \
  --bootstrap-server ${brokers} \
  --producer.config /etc/kafka/secrets/admin-scram.properties \
  --topic ${1} \
  --property parse.key=true \
  --property key.separator=:"
