package de.tum.i13.server.kv;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;

public interface KVCommunicator {

    /**
     * Disconnects the socket, if it is currently connected, If the socket is disconnected, the method does nothing.
     *
     * @throws Exception if there is an error during the disconnect.
     */
    public void disconnect() throws Exception;

    /**
     * Connects to the specified host on the specified port, if the socket is currently not connected. If the socket
     * is currently connected, the method does nothing.
     *
     * @param host The host to connect to.
     * @param port The port to connect to.
     * @throws Exception if there is an error during the disconnect.
     */
    public void connect(String host, int port) throws Exception;

    /**
     * Sends data to the server, if the socket is connected. If the socket is currently not connected, the
     * method will do nothing.
     *
     * @param   data    Data to be sent to the server.
     * @throws Exception if there is an error while sending the data.
     */
    public void send(byte[] data) throws Exception;

    /**
     * Reads from the input stream until /r/n is read.
     *
     * @return byte array containing the read data
     * @throws Exception if there is an error while sending the data.
     */
    public byte[] receive() throws Exception;
}
