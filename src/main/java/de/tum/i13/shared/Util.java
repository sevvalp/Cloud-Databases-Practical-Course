package de.tum.i13.shared;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;

public class Util {
    public static int getFreePort() {

        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
