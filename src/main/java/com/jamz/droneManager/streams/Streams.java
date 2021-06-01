package com.jamz.droneManager.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.jamz.droneManager.streams.serdes.JSONSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.jamz.droneManager.streams.Streams.Constants.*;

public class Streams {

    private Topology topology;
    private KafkaStreams streams;
    private final Properties props = new Properties();

    public Streams() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-drone-manager");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        topology = buildTopology(props, false);
    }

    public Topology buildTopology(Properties props, boolean testing) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);

        final StreamsBuilder streamBuilder = new StreamsBuilder();

        streamBuilder.table(DRONE_STATUS_TOPIC, consumed, Materialized.as(DRONE_STORE_NAME));

        topology = streamBuilder.build();

        return topology;
    }

    public KafkaStreams start(Topology topology, Properties props) {
        streams = new KafkaStreams(topology, props);
        streams.start();
        return streams;
    }

    public static class Constants {
        // Topics
        public static final String DRONE_STATUS_TOPIC = "DroneStatus";

        // Internal Names
        public static final String DRONE_STORE_NAME = "DroneStateStore";
    }

    // Getters Setters

    public KafkaStreams getStreams() {
        return streams;
    }

    public Properties getProperties() {
        return props;
    }
}
