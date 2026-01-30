# Docker Compose Setup and Chaos Engineering Tools

## Docker Compose

- [One Zookeeper and 5 Kafka Brokers (docker-compose-services.yml)](docker-compose-services.yml)
- [Kafka Users (docker-compose-kafka-add-user.yml)](docker-compose-kafka-add-user.yml)
- [kafbat for administration (docker-compose-tools-kafbat.yml)](docker-compose-tools-kafbat.yml) - Open with http://localhost:7778/
- [The Spring Boot services with producer/pipe/consumer (docker-compose-apps-minimal.yml)](docker-compose-apps-minimal.yml)
  - producer: produces messages to Kafka topic `a1` - Actuator endpoints via http://localhost:9081
  - pipe: reads messages from `a1` and writes them to `b1` - Actuator endpoints via http://localhost:9082
  - consumer: reads messages from `b1` - Actuator endpoints via http://localhost:9083

### Kafka Cluster

- There is one Zookeeper node `zk-1` and 5 Kafka brokers `kafka-1` .. `kafka-5`
- Kafka brokers are configured with SCRAM-SHA-512 authentication
- The setup defines a user `admin` with access to all topics and consumer groups and the right to create topics
- The setup defines a user `user1` with access to all topics and consumer groups, but NOT the right to create topics
- The setup defines also users `broker`, `c3` and `metricsreporter` for internal/future usage

## Scripts

- `./kafka-list-topics.sh list|describe|<topic-to-describe>`: List topics or describe a specific topic
- `./kafka-list-groups.sh list|describe|<group-to-describe>`: List consumer groups or describe a specific group
- `./kafka-consumer-metrics.sh <consumer-group>`: Show statistics about a specific consumer group
- `./kafka-test-consumer-progress.sh <capture|compare> <consumer-group-name> [expected_count]`: Capture and compare consumer group offsets to measure consumption progress
- `./kafka-test-consumer-delta.sh {<consumer-group-name> <topic-name>}+`: Analyze read-process-write chains for duplicate and missing messages
- `./kafka-list-offsets-topic.sh <topic-name>`: List offsets for all partitions of a given topic
- `./kafka-produce-topic.sh <topic-name>`: Write test messages to a topic
- `./kafka-consume-topic.sh <topic-name>`: Read messages from a topic
