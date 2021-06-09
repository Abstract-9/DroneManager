package com.jamz.droneManager.coap;

import com.fasterxml.jackson.databind.JsonNode;

public class DroneMessage {

    public MessageType messageType;
    public JsonNode payload;

    public DroneMessage(MessageType messageType, JsonNode payload) {
        this.messageType = messageType;
        this.payload = payload;
    }

    public enum MessageType {
        // Incoming Message Types (from kafka)
        BAY_ASSIGNMENT("BayAssignment"),
        BAY_ACCESS_GRANTED("AccessGranted"),
        BAY_ACCESS_DENIED("AccessDenied"),
        PATH_ASSIGNMENT("PathAssignment"),
        JOB_ASSIGNMENT("AuctionClose"),
        JOB_AUCTION("AuctionOpen"),
        // Outgoing Message Types (to kafka)
        BAY_ASSIGNMENT_REQUEST("AssignmentRequest"),
        BAY_ACCESS_REQUEST("AccessRequest"),
        PLACE_BIDS("BidsPlaced"),
        PATH_PROPOSAL("PathProposal"),
        // To make switches work with reversed eventStrings
        NONE(null);

        public String eventString;
        MessageType(String eventString) {
            this.eventString = eventString;
        }

        public static MessageType fromEventString(String eventString) {
            for( MessageType messageType: MessageType.values()) {
                if (messageType.eventString.equals(eventString)) return messageType;
            }
            return NONE;
        }
    }


}
