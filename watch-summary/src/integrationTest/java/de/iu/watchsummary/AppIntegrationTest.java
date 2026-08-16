package de.iu.watchsummary;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs App's real topology (App.buildTopology) as an actual KafkaStreams
 * instance against a real broker + real Schema Registry - not
 * TopologyTestDriver's in-process simulation (see AppTest, which covers
 * the session-window/reduce/suppress branch logic fast and without infra).
 * This class exists to catch the things a synchronous, single-threaded
 * TopologyTestDriver run can't: real Avro round-trips through Schema
 * Registry, a real consumer group actually rebalancing/starting up, and
 * session windows closing off real broker-timestamp-driven stream time
 * instead of timestamps this test hands the driver directly.
 *
 * Needs the project's own docker-compose stack up first - same convention
 * reporting-output's integration tests use (see
 * ../../../reporting-output/README.md's "Tests" section and
 * tests/conftest.py's require_live_stack fixture): NOT a separate test-only
 * compose file, the real one, reachable on its host-mapped localhost ports.
 * See this module's own README.md "Tests" section for exactly what to run
 * before `gradle integrationTest`.
 */
class AppIntegrationTest {

    private static final String WATCH_EVENT_TOPIC = "de.iu.Watch.Event.V002";
    private static final String WATCH_SUMMARY_TOPIC = "de.iu.Watch.Summary.V002";
    private static final int SESSION_GAP_SECONDS = 3;  // short on purpose - keeps real-wall-clock waits sane
    private static final Path CLIENT_INPUT_SCHEMAS = Path.of("../client-input/schemas");

    private static final String KAFKA_BOOTSTRAP =
            System.getenv().getOrDefault("TEST_KAFKA_BOOTSTRAP", "localhost:9092");
    private static final String SCHEMA_REGISTRY_URL =
            System.getenv().getOrDefault("TEST_SCHEMA_REGISTRY_URL", "http://localhost:8081");

    private static Schema inputKeySchema;
    private static Schema inputValueSchema;
    private static KafkaProducer<GenericRecord, GenericRecord> producer;

    private String applicationId;
    private Path stateDir;
    private KafkaStreams streams;

    @BeforeAll
    static void requireLiveStack() throws IOException {
        // Fail fast with one clear message instead of every @Test in this
        // class timing out separately on a connection nothing's listening
        // on - same intent as reporting-output's require_live_stack fixture.
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000"))) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            for (String required : List.of(WATCH_EVENT_TOPIC, WATCH_SUMMARY_TOPIC)) {
                if (!topics.contains(required)) {
                    fail("Topic '" + required + "' doesn't exist on " + KAFKA_BOOTSTRAP + " - these are "
                            + "integration tests and need the real stack, including kafka-init's topic "
                            + "creation. Run `docker compose up -d kafka schema-registry kafka-init` (or "
                            + "`docker compose up -d` for the full stack) from the repo root first.");
                }
            }
        } catch (Exception e) {
            fail("Kafka not reachable at " + KAFKA_BOOTSTRAP + " - these are integration tests and need "
                    + "the real stack. Run `docker compose up -d kafka schema-registry kafka-init` from the "
                    + "repo root first. (" + e + ")");
        }

        inputKeySchema = loadSchema("de.iu.Watch.Event.V002-key.avsc");
        inputValueSchema = loadSchema("de.iu.Watch.Event.V002-value.avsc");

        Map<String, String> serdeConfig = Map.of("schema.registry.url", SCHEMA_REGISTRY_URL);
        Serde<GenericRecord> keySerde = new GenericAvroSerde();
        keySerde.configure(serdeConfig, true);
        Serde<GenericRecord> valueSerde = new GenericAvroSerde();
        valueSerde.configure(serdeConfig, false);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        producer = new KafkaProducer<>(producerProps, keySerde.serializer(), valueSerde.serializer());
    }

    @AfterAll
    static void closeProducer() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(10));
        }
    }

    private static Schema loadSchema(String filename) throws IOException {
        return new Schema.Parser().parse(Files.readString(CLIENT_INPUT_SCHEMAS.resolve(filename)));
    }

    @BeforeEach
    void startStreamsApp() throws Exception {
        // A unique application.id + state dir per test: a distinct consumer
        // group from any real watch-summary-service container that might
        // also be running against this same broker, and a clean slate
        // instead of stale RocksDB state from a previous test run.
        applicationId = "watch-summary-service-integration-test-" + UUID.randomUUID();
        stateDir = Files.createTempDirectory("watch-summary-it-");

        Topology topology = App.buildTopology(SCHEMA_REGISTRY_URL, SESSION_GAP_SECONDS, Path.of("schemas"));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        streams = new KafkaStreams(topology, props);
        CountDownLatch running = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                running.countDown();
            }
        });
        streams.start();
        assertTrue(running.await(30, TimeUnit.SECONDS), "streams app never reached RUNNING");
    }

    @AfterEach
    void stopStreamsApp() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
            streams.cleanUp();
        }
    }

    private GenericRecord key(String userId, String itemId) {
        GenericData.Record record = new GenericData.Record(inputKeySchema);
        record.put("user_id", userId);
        record.put("item_id", itemId);
        return record;
    }

    private GenericRecord value(String userId, String itemId, int watchedSeconds, String deviceType, long sessionEndedAtMs) {
        GenericData.Record record = new GenericData.Record(inputValueSchema);
        record.put("user_id", userId);
        record.put("item_id", itemId);
        record.put("watched_seconds", watchedSeconds);
        record.put("device_type", deviceType);
        record.put("session_ended_at", sessionEndedAtMs);
        return record;
    }

    private void produceHeartbeat(String userId, String itemId, int watchedSeconds, String deviceType) throws Exception {
        long now = System.currentTimeMillis();
        producer.send(new ProducerRecord<>(WATCH_EVENT_TOPIC, key(userId, itemId),
                value(userId, itemId, watchedSeconds, deviceType, now))).get(10, TimeUnit.SECONDS);
    }

    /**
     * Stream time is tracked per-*partition* (see README.md's "Stream
     * time, grace period, and lateness"), and Watch.Event.V002 has more
     * than one partition on the real broker (unlike TopologyTestDriver's
     * single simulated partition) - a trigger record keyed to advance one
     * key's window might land on a partition that has nothing to do with
     * it. Producing one explicitly to every partition guarantees whichever
     * partition the test's own key hashed to also gets pushed forward,
     * without needing to replicate the producer's own partitioner logic
     * to predict it.
     */
    private void produceTriggerOnEveryPartition() throws Exception {
        long now = System.currentTimeMillis();
        int partitionCount = producer.partitionsFor(WATCH_EVENT_TOPIC).size();
        List<java.util.concurrent.Future<?>> sends = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            String triggerId = "it-trigger-" + UUID.randomUUID();
            sends.add(producer.send(new ProducerRecord<>(WATCH_EVENT_TOPIC, partition,
                    key(triggerId, "it-trigger-item"), value(triggerId, "it-trigger-item", 1, "mobile", now))));
        }
        for (java.util.concurrent.Future<?> send : sends) {
            send.get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Polls de.iu.Watch.Summary.V002 from earliest until it sees a record
     * whose user_id matches userId (this topic is shared with every other
     * test/service on this broker, so a fresh "earliest" consumer will see
     * plenty of records that aren't ours - a random UUID user_id per test
     * is what makes filtering for "ours" unambiguous) or the timeout
     * elapses.
     */
    private List<GenericRecord> waitForSummaries(String userId, Duration timeout) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "watch-summary-it-reader-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> serdeConfig = Map.of("schema.registry.url", SCHEMA_REGISTRY_URL);
        Serde<GenericRecord> keySerde = new GenericAvroSerde();
        keySerde.configure(serdeConfig, true);
        Serde<GenericRecord> valueSerde = new GenericAvroSerde();
        valueSerde.configure(serdeConfig, false);

        List<GenericRecord> matches = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
                     new KafkaConsumer<>(consumerProps, keySerde.deserializer(), valueSerde.deserializer())) {
            consumer.subscribe(List.of(WATCH_SUMMARY_TOPIC));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    if (userId.equals(record.value().get("user_id").toString())) {
                        matches.add(record.value());
                    }
                }
            }
        }
        return matches;
    }

    @Test
    @Timeout(60)
    void singleHeartbeatOverRealKafkaSettlesIntoAMatchingSummary() throws Exception {
        String userId = "it-user-" + UUID.randomUUID();
        String itemId = "it-item-" + UUID.randomUUID();

        produceHeartbeat(userId, itemId, 120, "mobile");
        // Real wall-clock wait past the (short, test-only) session gap -
        // but unlike TopologyTestDriver, a live KafkaStreams instance's
        // stream time only advances when it actually processes a new
        // record on that partition; sleeping alone never closes the
        // window. A trigger heartbeat on an unrelated key, produced after
        // the sleep, is what pushes stream time past the boundary and
        // fires suppress(untilWindowCloses) for the first one - same
        // trick AppTest uses against TopologyTestDriver, just for a
        // genuinely different reason here (real stream time, not a
        // driver-controlled clock).
        Thread.sleep(Duration.ofSeconds(SESSION_GAP_SECONDS + 2L).toMillis());
        produceTriggerOnEveryPartition();

        List<GenericRecord> summaries = waitForSummaries(userId, Duration.ofSeconds(30));

        assertEquals(1, summaries.size(), "expected exactly one settled summary for " + userId);
        GenericRecord summary = summaries.get(0);
        assertEquals(itemId, summary.get("item_id").toString());
        assertEquals(120, summary.get("watched_seconds"));
        assertEquals("mobile", summary.get("device_type").toString());
    }

    @Test
    @Timeout(60)
    void heartbeatsInTheSameSessionOverRealKafkaKeepOnlyTheLatestOne() throws Exception {
        String userId = "it-user-" + UUID.randomUUID();
        String itemId = "it-item-" + UUID.randomUUID();

        produceHeartbeat(userId, itemId, 60, "mobile");
        Thread.sleep(500);  // well inside the gap - both heartbeats land in the same session window
        produceHeartbeat(userId, itemId, 90, "smart_tv");
        Thread.sleep(Duration.ofSeconds(SESSION_GAP_SECONDS + 2L).toMillis());
        produceTriggerOnEveryPartition();  // see that method's comment

        List<GenericRecord> summaries = waitForSummaries(userId, Duration.ofSeconds(30));

        assertEquals(1, summaries.size(), "same session must settle into exactly one summary, not two");
        GenericRecord summary = summaries.get(0);
        assertEquals(90, summary.get("watched_seconds"));
        assertEquals("smart_tv", summary.get("device_type").toString());
    }
}
