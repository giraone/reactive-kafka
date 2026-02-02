#!/bin/bash

app=jobs

rm -rf ${CONTAINER_DATA:-./data}/${app}/kafka-1
rm -rf ${CONTAINER_DATA:-./data}/${app}/kafka-2
rm -rf ${CONTAINER_DATA:-./data}/${app}/kafka-3
rm -rf ${CONTAINER_DATA:-./data}/${app}/kafka-4
rm -rf ${CONTAINER_DATA:-./data}/${app}/kafka-5
rm -rf ${CONTAINER_DATA:-./data}/${app}/zk-1