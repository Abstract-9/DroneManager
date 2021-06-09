package com.jamz.droneManager.coap;

import com.fasterxml.jackson.databind.JsonNode;
import com.jamz.droneManager.coap.resources.StatusResource;
import com.jamz.droneManager.streams.StreamsManager;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.network.config.NetworkConfig;

import java.net.InetSocketAddress;
import java.util.HashMap;

public class DroneServer extends CoapServer {

    private static final int COAP_PORT = NetworkConfig.getStandard().getInt(NetworkConfig.Keys.COAP_PORT);
    private static final int TCP_THREADS = NetworkConfig.getStandard().getInt(NetworkConfig.Keys.TCP_WORKER_THREADS);
    private static final int TCP_IDLE_TIMEOUT = NetworkConfig.getStandard()
            .getInt(NetworkConfig.Keys.TCP_CONNECTION_IDLE_TIMEOUT);

    public DroneServer() {
        // Bind to all interfaces
        CoapEndpoint.Builder builder = new CoapEndpoint.Builder();
        builder.setInetSocketAddress(new InetSocketAddress("0.0.0.0", COAP_PORT));
        builder.setNetworkConfig(NetworkConfig.getStandard());
        addEndpoint(builder.build());
        add(new StatusResource(this));
    }
}
