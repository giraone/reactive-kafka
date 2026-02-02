package com.giraone.kafka.pipeline.service;

import com.giraone.kafka.pipeline.KafkaPipelineApplication;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for Kafka integration tested.
 * This is basically a simplified copy from the reactor-kafka code of
 * <a href="https://github.com/reactor/reactor-kafka/blob/main/src/test/java/reactor/kafka/AbstractKafkaTest.java">AbstractKafkaTest.java</a>.
 * The main differences are
 * <ul>
 *     <li>Message keys are String not Integer</li>
 * </ul>
 */
public abstract class AbstractKafkaIntTest {

    protected static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.6"))
        // DockerImageName.parse("confluentinc/cp-enterprise-kafka:7.6.6")
        //    .asCompatibleSubstituteFor("confluentinc/cp-kafka"))
        .withNetwork(null)
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withReuse(false);

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaIntTest.class);

    protected static final long DEFAULT_TEST_TIMEOUT_MS = 20000;
    protected static final long REQUEST_TIMEOUT_MILLIS = 3000;
    protected static final long SESSION_TIMEOUT_MILLIS = 12000;
    protected static final long HEARTBEAT_INTERVAL_MILLIS = 3000;
    protected static final int PARTITIONS = 2;

    protected final List<List<String>> receivedMessagesPerPartition = new ArrayList<>(PARTITIONS);
    protected final List<List<ConsumerRecord<String, String>>> receivedRecordsPerPartition = new ArrayList<>(PARTITIONS);

    @Autowired
    KafkaPipelineApplication kafkaPipelineApplication; // We need a bean to wait for context startup

    @DynamicPropertySource
    protected static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> {
            waitForContainerStart();
            return KAFKA.getBootstrapServers();
        });
    }

    protected static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    protected static void waitForContainerStart() {

        if (!KAFKA.isCreated() || !KAFKA.isRunning()) {
            LOGGER.info("STARTING Kafka broker");
            KAFKA.start();
            LOGGER.info("STARTED Kafka broker {}", KAFKA.getBootstrapServers());
        }
    }

    /**
     * Each test class has its own id.
     */
    protected String getClientId() {
        return this.getClass().getSimpleName();
    }

    @BeforeEach
    public final void setUpAbstractKafkaTest() {
        waitForContainerStart();
        resetMessages();
    }

    protected Properties producerProps(String clientId) {
        // Create properties for the Kafka consumer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(REQUEST_TIMEOUT_MILLIS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    protected Properties consumerProps(String groupId, String clientId) {
        // Create properties for the Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(SESSION_TIMEOUT_MILLIS));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(HEARTBEAT_INTERVAL_MILLIS));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    protected Consumer<String, String> createConsumer(String topic) {
        // More or less random group id to avoid conflicts
        final String groupId = this.getClass().getSimpleName() + "-" + System.nanoTime();
        Properties consumerProps = consumerProps(groupId, getClientId());
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic));
        // Do an initial poll to join the group and get the assignment
        consumer.poll(Duration.ofMillis(REQUEST_TIMEOUT_MILLIS));
        return consumer;
    }

    protected void createNewTopic(String topic) {

        LOGGER.info("Creating topic {}", topic);
        try (
            AdminClient adminClient = KafkaAdminClient.create(
                Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
            )
        ) {
            adminClient.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1)))
                .all()
                .get(DEFAULT_TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (e.getCause() != null && e.getCause() instanceof TopicExistsException) {
                LOGGER.warn("{} {}", e.getMessage(), e.getClass());
            } else {
                throw new RuntimeException(e);
            }
        }
        waitForTopicAndResetMessages(topic);
    }

    protected void resetMessages() {
        receivedMessagesPerPartition.clear();
        receivedRecordsPerPartition.clear();
        for (int i = 0; i < PARTITIONS; i++) {
            receivedMessagesPerPartition.add(new ArrayList<>());
        }
        for (int i = 0; i < PARTITIONS; i++) {
            this.receivedRecordsPerPartition.add(new ArrayList<>());
        }
    }

    protected void waitForTopicAndResetMessages(String topic) {
        waitForTopic(topic);
        resetMessages();
    }

    protected void onReceive(ConsumerRecord<String, String> consumerRecord) {
        receivedMessagesPerPartition.get(consumerRecord.partition()).add(consumerRecord.key());
        receivedRecordsPerPartition.get(consumerRecord.partition()).add(consumerRecord);
    }

    protected void waitForMessages(Consumer<String, String> consumer, Integer expectedCount) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Wait for {} message(s) in \"{}\".", expectedCount == null ? "any number of" : expectedCount, consumer.assignment());
        }
        int readAsMany = expectedCount != null ? expectedCount : 1_000;
        int receivedCount = 0;
        long endTimeMillis = System.currentTimeMillis() + DEFAULT_TEST_TIMEOUT_MS;
        while (receivedCount < readAsMany && System.currentTimeMillis() < endTimeMillis) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(this::onReceive);
            receivedCount += records.count();
        }

        if (expectedCount != null) {
            assertThat(receivedCount).isEqualTo(expectedCount);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertThat(records.isEmpty()).isTrue();
        }
    }

    protected RecordMetadata send(String topic, Tuple2<String, String> message) throws Exception {

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps(getClientId()))) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message.getT1(), message.getT2());
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get(1, TimeUnit.MINUTES);
            LOGGER.info("Sent message with key={} to topic={} with partition={} and offset={}",
                message.getT1(), topic, recordMetadata.partition(), recordMetadata.offset());
            return recordMetadata;
        }
    }

    protected void sendMessagesAndAssertReceived(String topic, Consumer<String, String> consumer,
                                                 List<Tuple2<String, String>> messagesToSend,
                                                 List<Tuple2<String, String>> expectedMessages) throws Exception {

        for (Tuple2<String, String> message : messagesToSend) {
            send(topic, message);
        }
        // We have to wait some time. We use at least the producer request timeout.
        Thread.sleep(REQUEST_TIMEOUT_MILLIS);
        assertReceived(consumer, expectedMessages);
    }

    protected void assertReceived(Consumer<String, String> consumer,
                                  List<Tuple2<String, String>> expectedMessages) {

        waitForMessages(consumer, expectedMessages.size());
        final AtomicInteger i = new AtomicInteger();
        final AtomicInteger partition = new AtomicInteger();
        receivedRecordsPerPartition.forEach(recordListOfPartition -> {
            recordListOfPartition.forEach(consumerRecord -> {
                LOGGER.info("Partition={}: {} -> {}", partition.getAndIncrement(), consumerRecord.key(), consumerRecord.value());
                String expectedKey = expectedMessages.get(i.get()).getT1();
                String expectedBody = expectedMessages.get(i.getAndIncrement()).getT2();
                assertThat(consumerRecord.key()).isEqualTo(expectedKey);
                assertThat(consumerRecord.value()).isEqualTo(expectedBody);
            });
        });
    }

    protected List<ConsumerRecord<String, String>> getAllConsumerRecords() {

        return receivedRecordsPerPartition.stream().reduce((a, l) -> {
            a.addAll(l);
            return a;
        }).orElse(Collections.emptyList());
    }

    //------------------------------------------------------------------------------------------------------------------

    private void waitForTopic(String topic) {
        Properties props = producerProps(getClientId());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int maxRetries = 10;
            boolean done = false;
            for (int i = 0; i < maxRetries && !done; i++) {
                List<PartitionInfo> partitionInfo = producer.partitionsFor(topic);
                done = !partitionInfo.isEmpty();
                for (PartitionInfo info : partitionInfo) {
                    if (info.leader() == null || info.leader().id() < 0) {
                        done = false;
                        break;
                    }
                }
            }
            Assertions.assertTrue(done, "Timed out waiting for topic");
        }
    }
}
