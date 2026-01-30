# Reactive Spring Boot Kafka using atleon-kafka or reactor-kafka

There are two implementations of Reactive Kafka clients for Spring Boot:
- [atleon-kafka]() - GIT branch **main** and **atleon**
- [reactor-kafka]() - GIT branch **reactor**

Both implementations can be used and compared in a *Spring Boot* based application setup with Kafka brokers provided by *Docker Compose*.

## Setup

For Docker / Kafka Cluster see [docker/README.md](docker/README.md).

```bash
# Build
mvn clean package
mvn jib:dockerBuild

# Initialize Docker environment
cd docker
# Kafka Brokers only
./full-setup.sh none
# Kafka Brokers and produce/pipe/consume demo apps
./full-setup.sh minimal

# Without setup
docker-compose -f docker-compose-services.yml up -d
docker-compose -f docker-compose-apps.yml up -d

# Check logs
./open-terms.sh
```

## Config

- [application.yml](src/main/resources/application.yml)
- [pom.xml](pom.xml)

## Tests

The unit tests are Spring-Kafka-, Reactor-Kafka and Atleon-Kafka-free and based on `org.apache.kafka`.