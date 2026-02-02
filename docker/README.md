# Docker Compose Setup and Chaos Engineering Tools

## Docker Data

All scripts and compose files assume there is a root folder for the container data.
The environment variable `CONTAINER_DATA` must point to this folder!

If the Docker Kafka cluster is corrupt, or you want to start from scratch, create a new empty this folder,
point the environment variable `CONTAINER_DATA` to this new folder and run `./data-setup.sh` to create
the needed sub-folders. E.g.:

```bash
mkdir -p /tmp/containers2/data2
export CONTAINER_DATA=/tmp/containers/data2
./data-setup.sh
```

## Docker Compose

- [One Zookeeper and 5 Kafka Brokers (docker-compose-services.yml)](docker-compose-services.yml)
- [Kafka Users (docker-compose-kafka-add-user.yml)](docker-compose-kafka-add-user.yml)
- [kafbat for administration (docker-compose-tools-kafbat.yml)](docker-compose-tools-kafbat.yml) - Open with http://localhost:7778/
- [The Spring Boot services with producer/pipe/consumer (docker-compose-apps-minimal.yml)](docker-compose-apps-minimal.yml)
  - producer: produces messages to Kafka topic `a1` - Actuator endpoints via http://localhost:9081
  - pipe: reads messages from `a1` and writes them to `b1` - Actuator endpoints via http://localhost:9082
  - consumer: reads messages from `b1` - Actuator endpoints via http://localhost:9083
- [Spring Boot App Produce Only](docker-compose-app-produce.yml)
- [Spring Boot App Pipe Only](docker-compose-app-pipe.yml)
- [Spring Boot App Consume Only](docker-compose-app-consume.yml)

### Kafka Cluster

- There is one Zookeeper node `zk-1` and 5 Kafka brokers `kafka-1` .. `kafka-5`
- Kafka brokers are configured with SCRAM-SHA-512 authentication
- The setup defines a user `admin` with access to all topics and consumer groups and the right to create topics
- The setup defines a user `user1` with access to all topics and consumer groups, but NOT the right to create topics
- The setup defines also users `broker`, `c3` and `metricsreporter` for internal/future usage

## Docker vs. Podman

The scripts are written to support both. There must be an alias `docker-compose` pointing
to either `docker compose` or `podman-compose`.

## Topology and Topics

The default topology with 3 services is:

```
produce --> Topic a1 <-- pipe --> Topic b1 <-- consume
```

Change the compose files for adapting the topology and topics as needed.
E.g. change `APPLICATION_TOPIC_B: b1` to `APPLICATION_TOPIC_B: a1` and
start only `produce` and `consume` services to have only producer and consumer working on the same topic:

```
produce --> Topic a1 <-- consume
```

## Scripts

- `./kafka-list-topics.sh list|describe|<topic-to-describe>`: List topics or describe a specific topic
- `./kafka-list-groups.sh list|describe|<group-to-describe>`: List consumer groups or describe a specific group
- `./kafka-consumer-metrics.sh <consumer-group>`: Show statistics about a specific consumer group
- `./kafka-test-consumer-progress.sh <capture|compare> <consumer-group-name> [expected_count]`: Capture and compare consumer group offsets to measure consumption progress
- `./kafka-test-consumer-delta.sh {<consumer-group-name> <topic-name>}+`: Analyze read-process-write chains for duplicate and missing messages
- `./kafka-list-offsets-topic.sh <topic-name>`: List offsets for all partitions of a given topic
- `./kafka-produce-topic.sh <topic-name>`: Write test messages to a topic
- `./kafka-consume-topic.sh <topic-name>`: Read messages from a topic
- `./docker-delete-none-images.sh`: Clean docker images with `<none>` tag (dangling images, when `mvn jib:dockerBuild` was used multiple times)

