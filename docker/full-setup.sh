#!/bin/bash

if [[ $# -lt 1 ]]; then
  echo "Full setup of docker/podman environment"
  echo "Usage: $0 [--no-tools] <minimal|none>]"
  exit 1
fi

noTools=false
if [[ "$1" == "--no-tools" ]]; then
  noTools=true
  shift
fi

YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

apps="$1"

docker-compose -f docker-compose-services.yml up -d

echo "${BLUE}Wait 20 seconds, till the zookeeper is ready ...${NC}"
sleep 20

echo "${YELLOW}Creating Kafka users ..."
podman-compose -f docker-compose-kafka-add-user.yml up -d

echo "${BLUE}Wait 40 seconds, till the brokers are ready ...${NC}"
sleep 40

echo "${YELLOW}Creating kafka topics ...${NC}"
./kafka-create-topics.sh

if [[ "$noTools" == "false" ]]; then
  echo "${YELLOW}Starting tools (kafbat, ...).${NC}"
  podman-compose -f docker-compose-tools-kafbat.yml up -d
fi

if [[ "$apps" == "minimal" ]]; then
  echo "${YELLOW}Starting apps (minimal).${NC}"
  podman-compose -f docker-compose-apps-minimal.yml up -d
fi