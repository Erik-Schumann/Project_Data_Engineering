package de.iu.watchsummary;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * The one genuine stream-processing job in this project - see
 * ../README.md for why this earns Kafka Streams where the other
 * reporting-output jobs (plain scheduled SQL) don't.
 *
 * de.iu.Watch.Event.V002 now carries a message every tick for every
 * in-progress watch (client-input's heartbeat, see
 * ../../client-input/README.md), plus whichever one happens to be last for
 * a given (user_id, item_id) - there's no separate "finished" event on the
 * wire. This job session-windows on inactivity to find that last one and
 * republishes exactly one settled WatchSummaryEvent per watch to
 * de.iu.Watch.Summary.V002, once the gap closes - not a continuous stream
 * of updates. No aggregation/counting: the reducer only ever keeps
 * "whichever record has the later session_ended_at," never merges fields
 * from two records together, so two watches of the same item stay two
 * separate outputs as long as they're more than SESSION_GAP_SECONDS apart.
 */
public class App {

    private static final String WATCH_EVENT_TOPIC = "de.iu.Watch.Event.V002";
    private static final String WATCH_SUMMARY_TOPIC = "de.iu.Watch.Summary.V002";
    private static final String SCHEMAS_DIR = System.getenv().getOrDefault("SCHEMAS_DIR", "/app/schemas");
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    public static void main(String[] args) throws IOException {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
        int sessionGapSeconds = Integer.parseInt(System.getenv().getOrDefault("SESSION_GAP_SECONDS", "10"));

        Map<String, String> serdeConfig = Map.of(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // isKey matters here, not just cosmetically: it picks which Schema
        // Registry subject (<topic>-key vs <topic>-value) a serde registers
        // against. Reusing one serde instance for both key and value slots
        // (an earlier version of this file did) makes both register under
        // the same "-value" subject and collide the moment the value's
        // schema (5 fields) conflicts with the key's (2 fields) under
        // BACKWARD compatibility - two separate instances per role, always.
        Serde<GenericRecord> watchEventKeySerde = new GenericAvroSerde();
        watchEventKeySerde.configure(serdeConfig, true);
        Serde<GenericRecord> watchEventValueSerde = new GenericAvroSerde();
        watchEventValueSerde.configure(serdeConfig, false);

        Serde<GenericRecord> watchSummaryKeySerde = new GenericAvroSerde();
        watchSummaryKeySerde.configure(serdeConfig, true);
        Serde<GenericRecord> watchSummaryValueSerde = new GenericAvroSerde();
        watchSummaryValueSerde.configure(serdeConfig, false);

        Schema summaryKeySchema = loadSchema("de.iu.Watch.Summary.V002-key.avsc");
        Schema summaryValueSchema = loadSchema("de.iu.Watch.Summary.V002-value.avsc");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<Windowed<GenericRecord>, GenericRecord> settled = builder
                // Already keyed {user_id, item_id} by the source topic - no
                // selectKey, no repartition topic.
                .stream(WATCH_EVENT_TOPIC, Consumed.with(watchEventKeySerde, watchEventValueSerde))
                .groupByKey(Grouped.with(watchEventKeySerde, watchEventValueSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(sessionGapSeconds)))
                .reduce(
                        (oldVal, newVal) -> eventTimestamp(newVal) >= eventTimestamp(oldVal) ? newVal : oldVal,
                        Materialized.with(watchEventKeySerde, watchEventValueSerde))
                // Emit once, when the session window actually closes - not
                // on every intermediate update.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        settled.toStream()
                .map((windowedKey, latest) -> KeyValue.pair(
                        toSummaryKey(latest, summaryKeySchema),
                        toSummaryEvent(latest, summaryValueSchema)))
                .to(WATCH_SUMMARY_TOPIC, Produced.with(watchSummaryKeySerde, watchSummaryValueSerde));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "watch-summary-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // No kPow Streams Agent registration here: kPow's Kafka Streams
        // view (topology, per-instance state, lag) is an Enterprise-only
        // feature - this stack runs kpow-ce (Community Edition), which
        // can't render it regardless of client-side wiring. This app is
        // still visible in kPow as a plain consumer group ("watch-summary-
        // service") the same way any consumer is - see watch-summary/README.md.
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    private static long eventTimestamp(GenericRecord record) {
        return (long) record.get("session_ended_at");
    }

    // The reducer's output is already shaped exactly like the output
    // schema - this is a field copy, not a real transform. The value only
    // ever changes because the topology decided *this* record is the one
    // worth keeping, never by merging two records' fields together.
    private static GenericRecord toSummaryKey(GenericRecord source, Schema schema) {
        GenericData.Record key = new GenericData.Record(schema);
        key.put("user_id", source.get("user_id"));
        key.put("item_id", source.get("item_id"));
        return key;
    }

    private static GenericRecord toSummaryEvent(GenericRecord source, Schema schema) {
        GenericData.Record value = new GenericData.Record(schema);
        value.put("user_id", source.get("user_id"));
        value.put("item_id", source.get("item_id"));
        value.put("watched_seconds", source.get("watched_seconds"));
        value.put("device_type", source.get("device_type"));
        value.put("session_ended_at", source.get("session_ended_at"));
        return value;
    }

    private static Schema loadSchema(String filename) throws IOException {
        String content = Files.readString(Path.of(SCHEMAS_DIR, filename));
        return new Schema.Parser().parse(content);
    }
}
