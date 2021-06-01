package com.jamz.droneManager;

import com.jamz.droneManager.coap.DroneServer;
import com.jamz.droneManager.streams.Streams;

public class DroneManager {

    public static void main(final String[] args) {
        Streams streamManager = new Streams();
        DroneServer coapServer = new DroneServer(streamManager);

    }
}
