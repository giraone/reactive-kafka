#!/bin/bash

if [[ $# -lt 1 ]] ; then
    echo "Usage: $0 list|describe|<group-to-describe> [<broker_id]"
    echo " e.g.: $0 consume"
    echo " e.g.: $0 pipe"
    exit 1
fi

broker_id=${2:-1}
broker_name="kafka-${broker_id}"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"

if [[ "$1" == "list" ]]; then
  docker exec -t ${broker_name} kafka-consumer-groups \
        --bootstrap-server ${brokers} \
        --command-config /etc/kafka/secrets/admin-scram.properties \
        --list
elif [[ "$1" == "describe" ]]; then
  groups=$(docker exec ${broker_name} kafka-consumer-groups \
      --bootstrap-server ${brokers} \
      --command-config /etc/kafka/secrets/admin-scram.properties \
      --list)
  IFS=
  echo $groups | while read group; do
    if [[ $group != __* ]]; then
      docker exec -t ${broker_name} kafka-consumer-groups \
          --bootstrap-server ${brokers} \
          --command-config /etc/kafka/secrets/admin-scram.properties \
          --describe --group "$group"
    fi
  done
else
  docker exec -t ${broker_name} kafka-consumer-groups \
        --bootstrap-server ${brokers} \
        --command-config /etc/kafka/secrets/admin-scram.properties \
        --describe --group "$1"
fi
