#!/bin/bash

if [[ $# -lt 1 ]] ; then
    echo "Usage: $0 <topic> <options>*"
    echo " e.g.: $0 b1"
    echo " e.g.: $0 a1 --property print.timestamp=true"
    echo " e.g.: $0 b1 --from-beginning --timeout-ms 2000 "
    echo " e.g.: $0 b1 --consumer-property group.id=my-group"
    exit 1
fi

topic="${1}"
shift

if [[ -n "$groupId" ]]; then
  echo "Using consumer group id: $groupId"
  groupId="--consumer-property group.id=$groupId"
fi

broker="kafka-1"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"

docker exec -it ${broker} bash -c "kafka-console-consumer \
  --bootstrap-server ${brokers} \
  --consumer.config /etc/kafka/secrets/admin-scram.properties \
  --property print.key=true \
  --property print.partition=true \
  --topic '${topic}' $*"
