package com.jamz.droneManager.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.jamz.droneManager.DroneManager;
import com.jamz.droneManager.coap.DroneMessage;
import com.jamz.droneManager.streams.serdes.JSONSerde;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Properties;

import static com.jamz.droneManager.streams.StreamsManager.Constants.*;
import static com.jamz.droneManager.coap.DroneMessage.MessageType.*;

public class StreamsManager {

    private Topology topology;
    private KafkaStreams streams;
    private final Properties props = new Properties();

    public StreamsManager() {
        // Application Config
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-drone-manager");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-419q3.us-east4.gcp.confluent.cloud:9092");
        // Security Config
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"PSAFM6VH7LWNGV5D\" password=\"DfAiu9RSyI/udfvUm9j3HUtxHEECfrR9+K7tE8NTCI5g1x2am9ZkRfFWUSf+uT8G\";");
        // Performance Config
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 2147483647);
        props.put("producer.confluent.batch.expiry.ms", 9223372036854775807L);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 300000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L);
        // Serdes config
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
        // Confluent Schema Registry for Java (TODO once I've defined it in confluent cloud)
//        props.put("basic.auth.credentials.source", "USER_INFO");
//        props.put("schema.registry.basic.auth.user.info", "<SCHEMA_REGISTRY_API_KEY>:<SCHEMA_REGISTRY_API_SECRET>");
//        props.put("schema.registry.url", "https://<SCHEMA_REGISTRY_ENDPOINT>");

        topology = buildTopology(props, false);
        start(topology, props);
    }

    public Topology buildTopology(Properties props, boolean testing) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);

        final StreamsBuilder streamBuilder = new StreamsBuilder();

        // Build bay assignment stream
        streamBuilder.stream(BAY_ASSIGNMENT_TOPIC, consumed).filter((String key, JsonNode value) ->
                value.get("eventType").textValue().equals(BAY_ASSIGNMENT.eventString) && DroneManager.hasDrone(key)
        ).foreach((String key, JsonNode value) -> {
            DroneMessage message = new DroneMessage(DroneMessage.MessageType.BAY_ASSIGNMENT, value);
            DroneManager.putDroneMessage(key, message);
        });

        // Build bay access stream
        streamBuilder.stream(BAY_ACCESS_TOPIC, consumed).filter((String key, JsonNode value) -> {
            String eventType = value.get("eventType").textValue();
            return DroneManager.hasDrone(key) &&
                    (eventType.equals(BAY_ACCESS_GRANTED.eventString) || eventType.equals(BAY_ACCESS_DENIED.eventString));
        }
        ).foreach((String key, JsonNode value) -> {
            String eventType = value.get("eventType").textValue();
            DroneMessage message;
            if (eventType.equals(BAY_ACCESS_GRANTED.eventString)) {
                message = new DroneMessage(BAY_ACCESS_GRANTED, value);
            } else {
                message = new DroneMessage(BAY_ACCESS_DENIED, value);
            }
            DroneManager.putDroneMessage(key, message);
        });

        // Build job assignment stream
        // Since only job assignments are posted here, we don't need to filter by eventType
        streamBuilder.stream(JOB_ASSIGNMENT_TOPIC, consumed).foreach(DroneManager::handleBidClose);

        // Build bidding stream
        streamBuilder.stream(JOB_BID_TOPIC, consumed).filter((String key, JsonNode value) ->
            value.has("eventType") && value.get("eventType").textValue().equals(JOB_AUCTION.eventString)
        ).mapValues(DroneManager::generateBids).to(JOB_BID_TOPIC);

        // Build flight path stream
        streamBuilder.stream(FLIGHT_PATH_TOPIC, consumed).filter((String key, JsonNode value) ->
                value.has("eventType") && value.get("eventType").textValue().equals(PATH_ASSIGNMENT.eventString) && DroneManager.hasDrone(key)
        ).foreach((String key, JsonNode value) ->
            DroneManager.putDroneMessage(key, new DroneMessage(PATH_ASSIGNMENT, value))
        );
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
        public static final String BAY_ACCESS_TOPIC = "BayAccess";
        public static final String BAY_ASSIGNMENT_TOPIC = "BayAssignment";
        public static final String FLIGHT_PATH_TOPIC = "FlightPath";
        public static final String JOB_BID_TOPIC = "JobBids";
        public static final String JOB_ASSIGNMENT_TOPIC = "JobAssignment";
    }

    // Getters Setters

    public KafkaStreams getStreams() {
        return streams;
    }

    public Properties getProperties() {
        return props;
    }
}
