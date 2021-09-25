package com.jamz.droneManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jamz.droneManager.coap.DroneMessage;
import com.jamz.droneManager.coap.DroneServer;
import com.jamz.droneManager.streams.StreamsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

public class DroneManager {

    private static StreamsManager streamsManager;
    private static DroneServer coapServer;

    private static final JsonNodeFactory factory = new JsonNodeFactory(true);

    private static final HashMap<String, JsonNode> droneStatuses = new HashMap<>();
    private static final HashMap<String, JsonNode> activeJobs = new HashMap<>();
    private static final HashMap<String, ArrayList<DroneMessage>> droneMessages = new HashMap<>();
    private static Logger log;

    public static void main(final String[] args) {
        log = LoggerFactory.getLogger(DroneManager.class);
        streamsManager = new StreamsManager();
        coapServer = new DroneServer();

        coapServer.start();

    }

    public static void putDroneData(String drone_id, JsonNode data) {
        droneStatuses.put(drone_id, data);
    }

    public static boolean hasDrone(String drone_id) {
        return droneStatuses.containsKey(drone_id);
    }

    public static void putDroneMessage(String drone_id, DroneMessage message) {
        ArrayList<DroneMessage> messages = droneMessages.containsKey(drone_id)
                ? droneMessages.get(drone_id) : new ArrayList<>();
        messages.add(message);
        droneMessages.put(drone_id, messages);
    }

    public static void handleBidClose(String jobID, JsonNode result) {
        if (!result.get("eventType").textValue().equals("JobAssignment")) return; // Skip messages that are assignment approval requests

        if (!hasDrone(result.get("drone_id").textValue())) { // Skip messages that aren't for the drones being managed
            ObjectNode droneMessage = new ObjectNode(factory); // Create message template

            // If this job is emitted normally, we'll have it in the active jobs map
            if (activeJobs.containsKey(jobID)) {
                droneMessage.put("eventType", "JobAssignment")
                        .set("job_waypoints", activeJobs.get(jobID).get("job_waypoints"));
            // if this job isn't emitted normally, it's for testing. Use the embedded waypoints.
            } else if (result.has("job_waypoints")) {
                droneMessage.put("eventType", "JobAssignment")
                        .set("job_waypoints", result.get("job_waypoints"));
            // If neither of those work, this job is malformed.
            } else {
                log.warn("Received malformed job request! Job ID " + jobID);
                return;
            }
            // Tell the drone it's got a job.
            putDroneMessage(result.get("drone_id").textValue(),
                    new DroneMessage(DroneMessage.MessageType.JOB_ASSIGNMENT, droneMessage));
        } else {
            activeJobs.remove(jobID); // If the job isn't assigned to one of the managed drones, remove it.
        }
    }

    public static JsonNode generateBids(String jobID, JsonNode auction) {
        log.info("Received a job auction: " + jobID + ". Available Drones: \n" + droneStatuses);
        activeJobs.put(jobID, auction);
        ObjectNode bidEvent = new ObjectNode(factory);
        bidEvent.put("eventType", "BidsPlaced");
        ArrayNode bids = bidEvent.putArray("bids");
        droneStatuses.forEach((String key, JsonNode value) -> {
            if (value.get("Flight_Controller_State").textValue().equals("STATE_IDLE")) {
                int level = value.get("Battery").intValue();
                bids.add(new ObjectNode(factory).put(
                        "ID", key
                ).put(
                        "value", level
                ));
            }
        });
        return bidEvent;
    }

    public static boolean hasMessages(String drone_id) {
        return droneMessages.get(drone_id) != null && !droneMessages.get(drone_id).isEmpty();
    }

    public static DroneMessage[] getMessages(String drone_id) {
        DroneMessage[] tmp = new DroneMessage[]{};
        tmp = droneMessages.get(drone_id).toArray(tmp);
        droneMessages.get(drone_id).clear();
        return tmp;
    }

    public static StreamsManager getStreamsManager() {
        return streamsManager;
    }

    public static DroneServer getDroneServer() {
        return coapServer;
    }
}
