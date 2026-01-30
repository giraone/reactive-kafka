#!/bin/bash

if [[ $# -lt 1 ]] ; then
    echo "Usage: $0 list|describe|offsets|<topic-to-describe>"
    echo " e.g.: $0 list"
    echo " e.g.: $0 describe"
    echo " e.g.: $0 a1"
    exit 1
fi

broker="kafka-1"
brokers="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092"

if [[ "$1" == "list" ]]; then
  docker exec -t ${broker} kafka-topics \
        --bootstrap-server ${brokers} \
        --command-config /etc/kafka/secrets/admin-scram.properties \
        --list
elif [[ "$1" == "describe" ]]; then
  topics=$(docker exec ${broker} kafka-topics \
      --bootstrap-server ${brokers} \
      --command-config /etc/kafka/secrets/admin-scram.properties \
      --list)
  IFS=
  echo $topics | while read topic; do
    if [[ $topic != __* ]]; then
      docker exec -t ${broker} kafka-topics \
          --bootstrap-server ${brokers} \
          --command-config /etc/kafka/secrets/admin-scram.properties \
          --describe --topic "$topic"
    fi
  done
elif [[ "$1" == "offsets" ]]; then
  topics=$(docker exec ${broker} kafka-topics \
      --bootstrap-server ${brokers} \
      --command-config /etc/kafka/secrets/admin-scram.properties \
      --list)
  IFS=
  echo $topics | while read topic; do
    if [[ $topic == __* ]]; then
      docker exec -t ${broker} kafka-topics \
          --bootstrap-server ${brokers} \
          --command-config /etc/kafka/secrets/admin-scram.properties \
          --describe --topic "$topic"
    fi
  done
else
  docker exec -t ${broker} kafka-topics \
        --bootstrap-server ${brokers} \
        --command-config /etc/kafka/secrets/admin-scram.properties \
        --describe --topic "$1"
fi
