#!/bin/bash

if [[ $# -eq 0 ]]; then
   echo "Usage: $0 <app> <sleepSecondsDown> <sleepSecondsUp> [command1] [command2]"
   echo " e.g. $0 pipe 10 20"
   echo " e.g. $0 consume 15 30 stop start"
   echo " e.g. $0 15 30 sigTerm restart"
   echo " e.g. $0 130 30 pause unpause"
   echo "Commands are"
   echo "- start/stop: Start/Stop the container (app) in a loop. Brutal way, because the Hostname is gone then. This is the default action."
   echo "- pause/unpause: Pause/Unpause the container (app) in a loop. Pause/unpause is like kill -SIGSTOP and kill -SIGCONT"
   echo "- sigTerm/restart: Kill the container (app) with SIGTERM and restart the container"
   echo "- sigKill/restart: Kill the container (app) with SIGKILL and restart the container"
   exit 1
fi

app="$1"
sleepSecondsDown=${2:-1}
sleepSecondsUp=${3:-20}
if [[ $# -gt 3 ]]; then
  # e.g. pause, unpause
  feature1="${4}"
  feature2="${5}"
else
  feature1=stop
  feature2=start
fi

echo "CHAOS for ${app}: ${feature1} in a loop ... and ${feature2} then ..."
while true; do
  echo -n "${feature1} "
  if [[ "${feature1}" == "sigTerm" ]]; then
    docker kill -s SIGTERM "${app}"
  elif [[ "${feature1}" == "sigKill" ]]; then
    docker kill -s SIGKILL "${app}"
  else
    docker "${feature1}" "${app}"
  fi
  echo "Waiting ${sleepSecondsDown} seconds" && sleep ${sleepSecondsDown}
  echo -n "${feature2} "
  docker "${feature2}" "${app}"
  echo "Waiting ${sleepSecondsUp} seconds" && sleep ${sleepSecondsUp}
done

