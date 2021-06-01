package com.jamz.droneManager.coap.resources;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
import com.jamz.droneManager.streams.Streams;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.server.resources.CoapExchange;

import java.util.Properties;

public class StatusResource extends CoapResource {

    private KafkaProducer<String, JsonNode> producer;
    private final JsonNodeDeserializer deserializer =
            (JsonNodeDeserializer) JsonNodeDeserializer.getDeserializer(JsonNode.class);

    public StatusResource(Streams streams) {
        super("StatusResource");
        getAttributes().setTitle("Status Resource");

        // Create Kafka Producer
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();

        final Properties producerConfig = new Properties();
        producerConfig.putAll(streams.getProperties());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "drone-manager-producer");
        producer = new KafkaProducer<>(producerConfig, Serdes.String().serializer(), jsonSerializer);
    }

    @Override
    public void handlePUT(CoapExchange exchange) {
        // Handling all done statuses in memory will be costly, I should run sqlite or similar in future

    }
}
