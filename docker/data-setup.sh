#!/bin/bash

app=jobs
mkdir -m 777 -p ${CONTAINER_DATA:-./data}/${app}

mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/zk-1
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/zk-1/data
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/zk-1/log
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/zk-1/secrets
cp kafka/secrets/zk-1/* ${CONTAINER_DATA:-./data}/${app}/zk-1/secrets

mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-1
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-2
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-3
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-4
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-5

mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-1/data
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-2/data
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-3/data
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-4/data
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-5/data

mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-1/secrets
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-2/secrets
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-3/secrets
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-4/secrets
mkdir -m 777 ${CONTAINER_DATA:-./data}/${app}/kafka-5/secrets

# Same secrets for all 5 brokers
cp kafka/secrets/kafka-1/* ${CONTAINER_DATA:-./data}/${app}/kafka-1/secrets
cp kafka/secrets/kafka-1/* ${CONTAINER_DATA:-./data}/${app}/kafka-2/secrets
cp kafka/secrets/kafka-1/* ${CONTAINER_DATA:-./data}/${app}/kafka-3/secrets
cp kafka/secrets/kafka-1/* ${CONTAINER_DATA:-./data}/${app}/kafka-4/secrets
cp kafka/secrets/kafka-1/* ${CONTAINER_DATA:-./data}/${app}/kafka-5/secrets