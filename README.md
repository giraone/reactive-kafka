# Reactive Spring Boot Kafka using atleon-kafka or reactor-kafka

There are two implementations of Reactive Kafka clients for Spring Boot:
- [atleon-kafka](https://github.com/atleon/atleon) - GIT branch **main** and **atleon**
- [reactor-kafka](https://github.com/reactor/reactor-kafka) - GIT branch **reactor**

Both implementations can be used and compared in a *Spring Boot* based application setup with Kafka brokers provided by *Docker Compose*.

## Comparison

See [Reactor vs. Atleon](reactor-vs-atleon.md).

## Setup

For more information on Docker / Kafka Cluster see [docker/README.md](docker/README.md).

Do this once:

```bash
# Build
mvn clean package
mvn jib:dockerBuild

# Initialize Docker environment
cd docker
# Setup docker environment with Kafka Brokers only
./full-setup.sh none
```

## Running Integration Tests with Docker Compose

### Run all 3 service (produce, pipe, consume) together

```bash
# Start services
docker-compose -f docker-compose-apps-minimal.yml up -d
# Check logs
./open-terms.sh
# Stop services
docker-compose -f docker-compose-apps-minimal.yml down
```

### Run producer once and test consumer behaviour

```bash
# Stop eventually running service containers
docker stop produce pipe consume
# Create 10000 events to a1
docker-compose -f docker-compose-app-produce.yml up -d && docker logs -f produce
# When "Finished producing 10000 events to a1 after X seconds" is shown then press `Ctrl-C` and stop producer:
docker-compose -f docker-compose-app-produce.yml down
# Capture the offsets of the consumer group (see docker/docker-compose-app-consume.yml for the defined consumer group)
# It should display: Total lag: 10000
./kafka-test-consumer-progress.sh capture Consume-1
# Run the consumer to consume the 10000 events
docker-compose -f docker-compose-app-consume.yml up -d && docker logs -f consume
```

The log will contain lines like:
```
2026-02-02 10:53:08,453 INFO  [ atleon-kafka-receive-consumer-consume-1] RECV/07: ops/partition=11514 ops/topic=11514 offset=3671 total/partition=1163 total/topic=1163
2026-02-02 10:53:08,453 INFO  [ atleon-kafka-receive-consumer-consume-1] TASK/**: ops=23019 total=2325
2026-02-02 10:53:08,457 INFO  [ atleon-kafka-receive-consumer-consume-1] CMMT/07: ops/partition=12287 ops/topic=12287 offset=3749 total/partition=1241 total/topic=1241
```

- where the lines starting with `RECV/`, `TASK/` and `CMMT/`, show the consumption progress periodically (defined by `APPLICATION_LOG_RATE`)
# whe

# Compare the consumption
./kafka-test-consumer-progress.sh compare Consume-1 10000
```

## Config

- [application.yml](src/main/resources/application.yml)
- [pom.xml](pom.xml)

## Unit Tests

The unit tests are Spring-Kafka-, Reactor-Kafka and Atleon-Kafka-free and based on `org.apache.kafka`.