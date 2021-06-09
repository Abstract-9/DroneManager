package com.jamz.droneManager.coap.resources;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jamz.droneManager.DroneManager;
import com.jamz.droneManager.coap.DroneMessage;
import com.jamz.droneManager.coap.DroneServer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.server.resources.CoapExchange;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.jamz.droneManager.streams.StreamsManager.Constants.*;

public class StatusResource extends CoapResource {

    private KafkaProducer<String, JsonNode> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonNodeFactory factory = new JsonNodeFactory(true);
    private DroneServer droneServer;

    public StatusResource(DroneServer droneServer) {
        super("StatusResource");
        getAttributes().setTitle("Status Resource");
        this.droneServer = droneServer;
        // Create Kafka Producer
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();

        final Properties producerConfig = new Properties();
        producerConfig.putAll(DroneManager.getStreamsManager().getProperties());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "drone-manager-producer");
        producer = new KafkaProducer<>(producerConfig, Serdes.String().serializer(), jsonSerializer);
    }

    @Override
    public void handlePUT(CoapExchange exchange) {
        // Handling all done statuses in memory will be costly, I should run sqlite or similar in future
        JsonNode payload;
        try {
            payload = mapper.readTree(new String(exchange.getRequestPayload(), StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
            return;
        }
        exchange.accept();
        String drone_id = exchange.getQueryParameter("drone_id");
        // Store the updated drone status
        DroneManager.putDroneData(drone_id, payload.get("status"));
        // Send it off to Kafka
        producer.send(new ProducerRecord<>(DRONE_STATUS_TOPIC, drone_id, payload.get("status")));

        // Determine messages from drone
        if (payload.has("messages")) translateIncoming(drone_id, (ArrayNode) payload.get("messages"));

        // Determine if messages need to go to the drone
        if (DroneManager.hasMessages(drone_id)) {
            ObjectNode response = new ObjectNode(factory);
            ArrayNode messages = response.putArray("messages");
            for (DroneMessage message : DroneManager.getMessages(drone_id)) {
                messages.add(new ObjectNode(factory).put(
                        "eventType", message.messageType.eventString).set("payload", message.payload));
            }
            try{
                StringWriter writer = new StringWriter();
                JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
                mapper.writeTree(jsonGenerator, response);
                exchange.respond(CoAP.ResponseCode.CONTENT, writer.toString());
            } catch (IOException e) {
                exchange.respond(CoAP.ResponseCode.INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void translateIncoming(String drone_id, ArrayNode messages) {
        for (JsonNode message : messages) {
            ProducerRecord<String, JsonNode> record = null;
            switch (DroneMessage.MessageType.fromEventString(message.get("eventType").textValue())) {
                case BAY_ASSIGNMENT_REQUEST:
                    record = new ProducerRecord<>(BAY_ASSIGNMENT_TOPIC, drone_id, message);
                    break;
                case BAY_ACCESS_REQUEST:
                    record = new ProducerRecord<>(BAY_ACCESS_TOPIC, drone_id, message);
                    break;
                case PATH_PROPOSAL:
                    record = new ProducerRecord<>(FLIGHT_PATH_TOPIC, drone_id, message);
                    break;
            }
            producer.send(record);
        }
    }
}
