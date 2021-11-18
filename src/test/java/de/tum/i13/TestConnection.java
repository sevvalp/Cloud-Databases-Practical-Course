package de.tum.i13;

import de.tum.i13.client.SocketCommunicator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.*;

public class TestConnection {


    private final SocketCommunicator sc = new SocketCommunicator();

    @BeforeAll
    private static void startServer() throws InterruptedException{
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    de.tum.i13.server.nio.StartSimpleNioServer.main(new String[]{});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start();
        Thread.sleep(2000);
    }

    @Test
    public void testConnectionSuccess() {
        Exception ex = null;
        try {
            sc.connect("127.0.0.1",5153);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertNull(ex);
    }

    @Test
    public void testConnectionUnknown() {
        Exception ex = null;
        try {
            sc.connect("unknown",5153);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertTrue(ex instanceof UnknownHostException);
    }

    @Test
    public void testConnectionIllegalPort() {
        Exception ex = null;
        try {
            sc.connect("127.0.0.1",123456789);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertTrue(ex instanceof IllegalArgumentException);
    }

    @Test
    public void testDisconnectSucess(){

        try {
            sc.connect("127.0.0.1",5153);
            if(sc.isConnected())
                sc.disconnect();
            else
                fail();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertFalse(sc.isConnected());
    }



}
