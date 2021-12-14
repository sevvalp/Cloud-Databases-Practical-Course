package de.tum.i13.client;

import de.tum.i13.server.kv.KVCommunicator;

import javax.naming.SizeLimitExceededException;

import static de.tum.i13.shared.Constants.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * Socket Communicator
 * This class is intended as a library to handle connection, interaction with a server, message passing, etc.
 *
 * @version 0.2
 * @since   2021-10-25
 *
 */
public class SocketCommunicator implements KVCommunicator {

    private final static Logger LOGGER = Logger.getLogger(SocketCommunicator.class.getName());

    private Socket mSocket;
    private OutputStream output;
    private InputStream input;
    private boolean isConnected;

    public SocketCommunicator() {
        mSocket = new Socket();
        output = null;
        input = null;
        isConnected = false;
    }

    /**
     * Disconnects the socket, if it is currently connected. If the socket is disconnected, the method does nothing.
     *
     * @throws IOException if there is an IOException during the disconnect.
     * @throws IllegalStateException if currently not connected to a KVServer.
     */
    @Override
    public void disconnect() throws IOException, IllegalStateException {
        // check if the socket is connected
        if (isConnected) {
            LOGGER.fine("Socket currently connected, trying to disconnect.");
            try {
                // close connection
                output.close();
                input.close();
                mSocket.close();
                LOGGER.info("Socket disconnected successfully");
            } catch (IOException e) {
                // error while closing, we assume the socket is dead and let the garbage collector handle it
                LOGGER.severe("IO Exception while closing the socket");
                throw e;
            }

            // update variables
            isConnected = false;
        } else {
            LOGGER.warning("Socket is not connected!");
            throw new IllegalStateException("Not connected to KVServer!");
        }
    }

    /**
     * Connects to the specified host on the specified port, if the socket is currently not connected. If the socket
     * is currently connected, the method does nothing.
     *
     * @param host The host to connect to.
     * @param port The port to use for the connection.
     * @throws IOException if there is an IOException during the connect.
     * @throws IllegalStateException if currently connected to a KVServer.
     */
    @Override
    public void connect(String host, int port) throws IOException, IllegalStateException {
        // check if the socket is disconnected
        if (!isConnected) {
            LOGGER.fine("Socket currently not connected, trying to connect");
            try {
                mSocket = new Socket(host, port);
                LOGGER.info("Socket connected successfully");
                isConnected = true;
                output = mSocket.getOutputStream();
                LOGGER.fine("Successfully got outputStream from socket");
                input = mSocket.getInputStream();
                LOGGER.fine("Successfully got inputStream from socket");
            } catch (IOException e) {
                LOGGER.severe("IO Exception while connecting");
                isConnected = false;
                e.printStackTrace();
                throw e;
            }
        } else {
            LOGGER.warning("Socket is currently connected!");
            throw new IllegalStateException("Currently connected to KVServer!");
        }
    }

    /**
     * Checks if the socket is currently connected.
     *
     * @return true, if the socket is currently connected
     *         false, otherwise
     */
    public boolean isConnected() {
        return isConnected;
    }

    /**
     * Sends message to the server. The message will be sanitized and finally sent using {@link #send(byte[])}.
     *
     * @param   message The message to be sent to the server
     * @throws IOException if there is an IOException while sending the message.
     * @throws IllegalStateException if not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is longer than 128 kB.
     */
    public void sendMessage(String message) throws IOException, IllegalStateException, SizeLimitExceededException {
        // replace \r\n with space, as this is the message delimiter of the server protocol
        message = message.replace("\r\n", "");
        message += "\r\n";
        LOGGER.info(String.format("Message to server: %s", message));
        //LOGGER.fine(String.format("Message in hex: %s", printHexBinary(message.getBytes())));

        // convert string to byte array
        send(message.getBytes(TELNET_ENCODING));
    }

    public void reconnect(String address, int port)  {
        try {
            disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            connect(address, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Sends data to the server, if the socket is connected. If the socket is currently not connected, the
     * method will do nothing.
     *
     * @param   data    Data to be sent to the server
     * @throws IOException if there is an IOException during the send.
     * @throws IllegalStateException if not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is loniger than 128 kB.
     */
    @Override
    public void send(byte[] data) throws IOException, IllegalStateException, SizeLimitExceededException {
        // check if the socket is connected
        if (isConnected) {
            if (data.length > 128000) {
                LOGGER.warning("Length of message too high! Not sending");
                throw new SizeLimitExceededException("Length of message to high!");
            }

            LOGGER.fine("Socket currently connected, trying to send data");
            // if server is unexpectedly disconnected, we will get an exception, which we catch
            try {
                output.write(data);
                output.flush();
                LOGGER.info("Successfully sent data");
            } catch (IOException e) {
                LOGGER.severe("IO Exception while sending data");
                isConnected = false;
                throw e;
            }
        } else {
            LOGGER.warning("Socket currently disconnected!");
            throw new IllegalStateException("Not connected to KVServer!");
        }
    }

    /**
     * Reads from the input stream until /r/n is read.
     *
     * @return byte array containing the read data
     * @throws IOException if there is an IOException during the receive.
     * @throws IllegalStateException if not connected to a KVServer.
     */
    @Override
    public byte[] receive() throws IOException, IllegalStateException {
        // check if the socket is connected
        if (isConnected) {
            LOGGER.fine("Socket currently connected, trying to read data");
            ByteArrayOutputStream data = new ByteArrayOutputStream();

            try {
                // iterate over input until \r\n is read
                byte readByte = 0;
                byte lastByte = 0;
                while (lastByte != '\r' || readByte != '\n') {
                    lastByte = readByte;
                    readByte = (byte) input.read();
                    data.write(readByte);
                }
            } catch (IOException e) {
                LOGGER.severe("IO Exception while reading input stream");
                isConnected = false;
                throw e;
            }
            LOGGER.info(String.format("Received %d bytes: \"%s\"", data.size(), new String(data.toByteArray(), StandardCharsets.ISO_8859_1)));
            //LOGGER.fine(String.format("In hex: %s", printHexBinary(data.toByteArray())));
            return data.toByteArray();

        } else {
            LOGGER.warning("Socket currently disconnected!");
            throw new IllegalStateException("Not connected to KVServer!");
        }
    }
    public String getAddress(){
        return mSocket.getInetAddress().toString().substring(1);
    }
    public int getPort(){
        return mSocket.getPort();
    }
}
