package com.jamz.droneManager;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jamz.droneManager.coap.DroneServer;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.coap.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DroneServerTests {

    private static final JsonNodeFactory factory = new JsonNodeFactory(true);

    @BeforeAll
    static void start() {
        DroneManager.main(null);
    }

    @Test
    void testResourceRequest() {
        Request request = Request.newPut();
        request.setURI("coap://localhost/status");
        request.getOptions().setUriQuery("?drone_id=0x1");
        request.setPayload("{\n  \"status\": {\n    \"battery\":100\n  }\n}");
        request.send();
        try {
            Response response = request.waitForResponse(1000000000);
            assertNotNull(response);
            assertEquals(CoAP.ResponseCode.VALID, response.getCode());
            assertTrue(DroneManager.hasDrone("0x1"));
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    @Test
    void testBayAssignmentRequest() {
        Request request = Request.newPut();
        request.getOptions().setUriQuery("?drone_id=0x1");
        request.setPayload("{\n  \"status\": {\n    \"battery\":100\n  }\n}");
    }
}
