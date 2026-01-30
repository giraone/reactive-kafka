#!/bin/bash

function usage {
   echo "Usage: $0 <mode=1|2|3|zookeeper|jute|jute-clean> <sleepSeconds1> <sleepSeconds2> [command1] [command2]"
   echo " e.g. $0 1"
   echo " e.g. $0 2 15 20 stop start"
   echo " e.g. $0 1 15 20 sigTerm restart"
   echo " e.g. $0 1 15 20 sigKill restart"
   echo " e.g. $0 3 130 20 pause unpause"
   echo " e.g. $0 zookeeper 15 30 stop start"
   echo "Commands are"
   echo "- start/stop: Start/Stop the container (Broker/Zookeeper) in a loop. Brutal way, because the Hostname is gone then. This is the default action."
   echo "- sigTerm/restart: Kill the container (Broker/Zookeeper) with SIGTERM and restart the container"
   echo "- sigKill/restart: Kill the container (Broker/Zookeeper) with SIGKILL and restart the container"
   echo "- pause/unpause: Pause/Unpause the container (Broker/Zookeeper) in a loop. Can be used only with podman --cgroup-manager=cgroupfs! Pause/unpause is like kill -SIGSTOP and kill -SIGCONT"
   echo "Modes are"
   echo " - 1: stop/starts 1 of 5 brokers."
   echo " - 2: stop/starts 2 of 5 brokers."
   echo " - 3: stop/starts 3 of 5 brokers."
   echo " - jute: fills/block the zookeeper."
   echo " - jute-clean: cleans the blocked zookeeper."
   echo "Default for sleep seconds is 20 seconds."
   exit 1
}

function performFeature {
  local feature=$1
  local sleepSeconds=$2
  local container=$3

  [[ $sleepSeconds != 0 ]] && echo "Waiting ${sleepSeconds} seconds before ${feature}" && sleep "${sleepSeconds}"
  echo -n "${feature} "
  if [[ "$feature" == "sigTerm" ]]; then
    docker kill -s SIGTERM "${container}"
  elif [[ "$feature" == "sigKill" ]]; then
    docker kill -s SIGKILL "${container}"
  else
    docker "${feature}" "${container}"
  fi
}

if [[ $# -eq 0 ]]; then
    usage
fi

mode=$1
sleepSeconds1=${2:-20}
sleepSeconds2=${3:-20}
if [[ $# -gt 3 ]]; then
  # e.g. pause, unpause
  feature1="${4}"
  feature2="${5}"
else
  feature1=stop
  feature2=start
fi

if [[ "$mode" == "1" ]]; then
  echo "CHAOS by ${feature1} 1 of 5 Brokers in a loop ... and ${feature2} them ..."
  while true; do
    for index in 1 2 3 4 5; do
      performFeature "${feature1}" "${sleepSeconds1}" "kafka-${index}"
      performFeature "${feature2}" "${sleepSeconds2}" "kafka-${index}"
    done
  done
elif [[ "$mode" == "2" ]]; then
  echo "CHAOS by ${feature1} 2 of 5 Brokers in a loop ... and ${feature2} them ..."
  typeset -i index=0
  while true; do
    performFeature "${feature1}" "${sleepSeconds1}" "kafka-$(((0 + index) % 5 + 1))"
    performFeature "${feature1}" 0                  "kafka-$(((1 + index) % 5 + 1))"
    performFeature "${feature2}" "${sleepSeconds2}" "kafka-$(((0 + index) % 5 + 1))"
    performFeature "${feature2}" 0                  "kafka-$(((1 + index) % 5 + 1))"
    (( index = (index + 2) % 5))
  done
elif [[ "$mode" == "3" ]]; then
  echo "CHAOS by ${feature1} 3 of 5 Brokers in a loop ... and ${feature2} them ..."
  while true; do
    performFeature "${feature1}" "${sleepSeconds1}" "kafka-$(((0 + index) % 5 + 1))"
    performFeature "${feature1}" 0                  "kafka-$(((1 + index) % 5 + 1))"
    performFeature "${feature1}" 0                  "kafka-$(((2 + index) % 5 + 1))"
    performFeature "${feature2}" "${sleepSeconds2}" "kafka-$(((0 + index) % 5 + 1))"
    performFeature "${feature2}" 0                  "kafka-$(((1 + index) % 5 + 1))"
    performFeature "${feature2}" 0                  "kafka-$(((2 + index) % 5 + 1))"
    (( index = (index + 3) % 5))
  done
elif [[ "$1" == "zookeeper" ]]; then
  echo "CHAOS by ${feature1} the Zookeeper in a loop ... and ${feature2} it ..."
  while true; do
    performFeature "${sleepSeconds1}" "${feature1}" zk-1
    performFeature "${sleepSeconds2}" "${feature2}" zk-1
  done
elif [[ "$mode" == "jute" ]]; then
  echo "CHAOS by filling the Zookeeper with lots of users in a loop ..."
  typeset -i i=0
  while true; do
    user="temp-$(date +%Y%m%d%H%M%S%N)"
    echo -n "Add user ... "
    ./kafka-user.sh kafka-1 add "$user" || exit 1
    (( i+=1 ))
    if (( i % 10 == 0 )); then
      sleep 1 # to allow Ctrl-C
    fi
  done
elif [[ "$mode" == "jute-clean" ]]; then
  echo "CHAOS by filling the Zookeeper with lots of users in a loop ..."
  typeset -i i=0
  while true; do
    user="temp-$(date +%Y%m%d%H%M%S%N)"
    echo -n "Add user ... "
    ./kafka-user.sh kafka-1 add "$user" || exit 1
    (( i+=1 ))
    if (( i % 10 == 0 )); then
      sleep 1 # to allow Ctrl-C
    fi
  done
else
   usage
fi

