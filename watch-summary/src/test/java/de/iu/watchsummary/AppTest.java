package de.iu.watchsummary;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives App's real topology (App.buildTopology, not a reimplementation)
 * through TopologyTestDriver - no broker, no Docker Compose stack needed.
 * Avro (de)serialization runs for real against a fresh mock:// schema
 * registry per test (io.confluent's in-memory MockSchemaRegistryClient,
 * selected by URL scheme), so this exercises the same GenericAvroSerde
 * path production uses, just without a network hop.
 *
 * Covers the three behaviors README.md documents as this job's whole
 * reason to exist: session-windowing on inactivity, "latest
 * session_ended_at wins, never merge/count," and emitting exactly once
 * per settled watch (not on every intermediate heartbeat).
 */
class AppTest {

    private static final String WATCH_EVENT_TOPIC = "de.iu.Watch.Event.V002";
    private static final String WATCH_SUMMARY_TOPIC = "de.iu.Watch.Summary.V002";
    private static final int SESSION_GAP_SECONDS = 10;
    private static final Path CLIENT_INPUT_SCHEMAS = Path.of("../client-input/schemas");

    private TopologyTestDriver driver;
    private TestInputTopic<GenericRecord, GenericRecord> inputTopic;
    private TestOutputTopic<GenericRecord, GenericRecord> outputTopic;
    private Schema inputKeySchema;
    private Schema inputValueSchema;

    @BeforeEach
    void setUp() throws IOException {
        // A unique mock:// scope per test - each is backed by its own
        // isolated in-memory registry, the same isolation a fresh
        // Postgres/SQLite fixture gives other services' tests.
        String mockSchemaRegistryUrl = "mock://watch-summary-test-" + UUID.randomUUID();
        Topology topology = App.buildTopology(mockSchemaRegistryUrl, SESSION_GAP_SECONDS, Path.of("schemas"));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "watch-summary-service-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(topology, props);

        Map<String, String> serdeConfig = Map.of("schema.registry.url", mockSchemaRegistryUrl);
        Serde<GenericRecord> keySerde = new GenericAvroSerde();
        keySerde.configure(serdeConfig, true);
        Serde<GenericRecord> valueSerde = new GenericAvroSerde();
        valueSerde.configure(serdeConfig, false);

        inputKeySchema = loadSchema("de.iu.Watch.Event.V002-key.avsc");
        inputValueSchema = loadSchema("de.iu.Watch.Event.V002-value.avsc");

        inputTopic = driver.createInputTopic(WATCH_EVENT_TOPIC, keySerde.serializer(), valueSerde.serializer());
        outputTopic = driver.createOutputTopic(WATCH_SUMMARY_TOPIC, keySerde.deserializer(), valueSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static Schema loadSchema(String filename) throws IOException {
        return new Schema.Parser().parse(Files.readString(CLIENT_INPUT_SCHEMAS.resolve(filename)));
    }

    private GenericRecord key(String userId, String itemId) {
        GenericData.Record record = new GenericData.Record(inputKeySchema);
        record.put("user_id", userId);
        record.put("item_id", itemId);
        return record;
    }

    private GenericRecord value(String userId, String itemId, int watchedSeconds, String deviceType, Instant sessionEndedAt) {
        GenericData.Record record = new GenericData.Record(inputValueSchema);
        record.put("user_id", userId);
        record.put("item_id", itemId);
        record.put("watched_seconds", watchedSeconds);
        record.put("device_type", deviceType);
        record.put("session_ended_at", sessionEndedAt.toEpochMilli());
        return record;
    }

    // Mirrors client-input/generator.py's produce() call: one heartbeat,
    // timestamped at its own session_ended_at (the field the reducer and
    // the session window both key off of).
    private void pipeHeartbeat(String userId, String itemId, int watchedSeconds, String deviceType, Instant sessionEndedAt) {
        inputTopic.pipeInput(key(userId, itemId), value(userId, itemId, watchedSeconds, deviceType, sessionEndedAt), sessionEndedAt);
    }

    @Test
    void singleHeartbeatSettlesIntoAMatchingSummaryOnceTheGapCloses() {
        Instant base = Instant.parse("2026-08-16T10:00:00Z");
        pipeHeartbeat("u1", "i1", 120, "mobile", base);
        // Unrelated key, timestamped past the gap - advances stream time
        // enough for u1/i1's window to close and be suppressed-emitted.
        pipeHeartbeat("u2", "i2", 1, "mobile", base.plusSeconds(SESSION_GAP_SECONDS + 5L));

        List<KeyValue<GenericRecord, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());  // the trigger's own window hasn't closed yet

        GenericRecord key = outputs.get(0).key;
        GenericRecord summary = outputs.get(0).value;
        assertEquals("u1", key.get("user_id").toString());
        assertEquals("i1", key.get("item_id").toString());
        assertEquals("u1", summary.get("user_id").toString());
        assertEquals("i1", summary.get("item_id").toString());
        assertEquals(120, summary.get("watched_seconds"));
        assertEquals("mobile", summary.get("device_type").toString());
        assertEquals(base.toEpochMilli(), summary.get("session_ended_at"));
    }

    @Test
    void heartbeatsInTheSameSessionKeepOnlyTheOneWithTheLaterSessionEndedAt() {
        Instant base = Instant.parse("2026-08-16T10:00:00Z");
        // Piped out of arrival order but the second one carries the later
        // session_ended_at - the reducer must key off that field, not
        // "whichever record arrived last."
        pipeHeartbeat("u1", "i1", 90, "smart_tv", base.plusSeconds(5));
        pipeHeartbeat("u1", "i1", 60, "mobile", base);
        pipeHeartbeat("u2", "i2", 1, "mobile", base.plusSeconds(SESSION_GAP_SECONDS + 10L));

        List<KeyValue<GenericRecord, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());  // never merges/counts - exactly one record survives

        GenericRecord key = outputs.get(0).key;
        GenericRecord summary = outputs.get(0).value;
        assertEquals("u1", key.get("user_id").toString());
        assertEquals("i1", key.get("item_id").toString());
        assertEquals(90, summary.get("watched_seconds"));
        assertEquals("smart_tv", summary.get("device_type").toString());
        assertEquals(base.plusSeconds(5).toEpochMilli(), summary.get("session_ended_at"));
    }

    @Test
    void heartbeatsMoreThanTheGapApartProduceTwoIndependentSummaries() {
        Instant base = Instant.parse("2026-08-16T10:00:00Z");
        pipeHeartbeat("u1", "i1", 60, "mobile", base);
        pipeHeartbeat("u1", "i1", 90, "mobile", base.plusSeconds(SESSION_GAP_SECONDS + 20L));
        pipeHeartbeat("u9", "i9", 1, "mobile", base.plusSeconds(2L * (SESSION_GAP_SECONDS + 20)));

        List<KeyValue<GenericRecord, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(2, outputs.size());  // a rewatch past the gap is two watches, never one merged/counted record
        // Same key both times - it's the values that must stay independent,
        // not the (user_id, item_id) identity.
        assertEquals("u1", outputs.get(0).key.get("user_id").toString());
        assertEquals("i1", outputs.get(0).key.get("item_id").toString());
        assertEquals(60, outputs.get(0).value.get("watched_seconds"));
        assertEquals("u1", outputs.get(1).key.get("user_id").toString());
        assertEquals("i1", outputs.get(1).key.get("item_id").toString());
        assertEquals(90, outputs.get(1).value.get("watched_seconds"));
    }

    @Test
    void nothingIsEmittedWhileTheSessionIsStillActive() {
        Instant base = Instant.parse("2026-08-16T10:00:00Z");
        // Heartbeats keep arriving inside the gap - the window never gets
        // the chance to close, so suppress(untilWindowCloses) must hold
        // everything back rather than emit on every intermediate update.
        for (int i = 0; i < 5; i++) {
            pipeHeartbeat("u1", "i1", 30 * (i + 1), "mobile", base.plusSeconds(i * 3L));
        }

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void aBridgingHeartbeatMergesTwoOtherwiseSeparateSessionWindows() {
        // A distinct Kafka Streams code path from "one window growing" or
        // "two independent windows": a record whose own gap to both an
        // already-open earlier window and a later one is <= SESSION_GAP_
        // SECONDS forces the session store to merge them into one, folding
        // all three together - not just append to whichever window arrived
        // most recently.
        //
        // Piping order matters here, and not just for realism: with zero
        // grace (ofInactivityGapWithNoGrace - see README.md's "Stream
        // time, grace period, and lateness"), a window closes the instant
        // stream time crosses its own boundary, with no lateness
        // tolerance at all. u1/i1's t=0 window's boundary is t=10; if t=15
        // were piped *before* the t=8 bridge, processing t=15 would
        // already push stream time past t=10 and close/emit the t=0
        // window on its own, before the bridge ever got a chance to merge
        // it with anything - two separate summaries, not the merge this
        // test exists to cover. Piping the bridge first keeps the t=0
        // window open long enough to actually be merged.
        Instant base = Instant.parse("2026-08-16T10:00:00Z");
        pipeHeartbeat("u1", "i1", 10, "mobile", base);                        // t=0
        pipeHeartbeat("u1", "i1", 20, "tablet", base.plusSeconds(8));         // t=8  - within gap of t=0, extends that window to [0,8]
        pipeHeartbeat("u1", "i1", 30, "desktop", base.plusSeconds(15));      // t=15 - within gap of the [0,8] window's end, merges into [0,15]
        pipeHeartbeat("u2", "i2", 1, "mobile", base.plusSeconds(2L * SESSION_GAP_SECONDS + 15));  // push stream time past the merged window's close

        List<KeyValue<GenericRecord, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());  // merged into exactly one summary, not two

        GenericRecord key = outputs.get(0).key;
        GenericRecord summary = outputs.get(0).value;
        assertEquals("u1", key.get("user_id").toString());
        assertEquals("i1", key.get("item_id").toString());
        // Latest session_ended_at across all three contributing records
        // (t=15) wins, regardless of the pairwise order the merge happened in.
        assertEquals(30, summary.get("watched_seconds"));
        assertEquals("desktop", summary.get("device_type").toString());
        assertEquals(base.plusSeconds(15).toEpochMilli(), summary.get("session_ended_at"));
    }
}
