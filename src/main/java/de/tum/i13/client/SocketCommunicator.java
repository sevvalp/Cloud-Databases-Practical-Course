package de.tum.i13.client;

import static de.tum.i13.shared.Constants.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * Socket Communicator
 * This class is intended as a library to handle connection, interaction with a server, message passing, etc.
 *
 * @version 0.1
 * @since   2021-10-25
 *
 */
public class SocketCommunicator {

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
     */
    public void disconnect() throws IOException {
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
            LOGGER.fine("Socket is not connected! Doing nothing.");
        }
    }

    /**
     * Connects to the specified host on the specified port, if the socket is currently not connected. If the socket
     * is currently connected, the method does nothing.
     *
     * @param   host    The host to connect to
     * @param   port    The port to use for the connection
     * @return  true,   if the socket connected successfully
     *          false,  otherwise
     */
    public boolean connect(String host, int port) throws IOException {
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
                return true;
            } catch (IOException e) {
                LOGGER.severe("IO Exception while connecting");
                isConnected = false;
                e.printStackTrace();
                throw e;
            }
        } else {
            LOGGER.fine("Socket is currently connected! Doing nothing");
        }
        return false;
    }

    /**
     * Checks if the socket is currently connected.
     *
     * @return  true,   if the socket is currently connected
     *          false,  otherwise
     */
    public boolean isConnected() {
        return isConnected;
    }

    /**
     * Sends data to the server, if the socket is connected. If the socket is currently not connected, the
     * method will do nothing.
     *
     * @param   data    Data to be sent to the server
     * @return  true,   if the data was sent successfully
     *          false,  otherwise
     */
    private boolean send(byte[] data) throws IOException {
        // check if the socket is connected
        if (isConnected) {
            LOGGER.fine("Socket currently connected, trying to send data");
            // if server is unexpectedly disconnected, we will get an exception, which we catch
            try {
                output.write(data);
                output.flush();
                LOGGER.info("Successfully sent data");
                return true;
            } catch (IOException e) {
                LOGGER.severe("IO Exception while sending data");
                isConnected = false;
                throw e;
            }
        } else {
            LOGGER.fine("Socket currently disconnected! Doing nothing");
        }
        return false;
    }

    /**
     * Reads from the input stream until /r/n is read.
     *
     * @return byte array containing the read data
     */
    private byte[] receive() throws IOException {
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
            return data.toByteArray();

        } else {
            LOGGER.fine("Socket currently disconnected! Doing nothing");
        }

        return new byte[0];
    }

    /**
     * Reads data from the socket using {@link #receive()} and decodes it.
     *
     * @return Decoded read message
     */
    public String receiveMessage() throws IOException {
        return new String(receive(), StandardCharsets.ISO_8859_1);
    }

    /**
     * Sends message to the server. The message will be sanitized and finally sent using {@link #send(byte[])}.
     *
     * @param   message The message to be sent to the server
     * @return  true,   if the message was sent successfully
     *          false,  otherwise
     */
    public boolean sendMessage(String message) throws IOException {
        // replace \r\n with space, as this is the message delimiter of the server protocol
        message = message.replace("\r\n", "");
        message += "\r\n";
        LOGGER.info(String.format("Message to server: %s", message));
        byte[] data;
        // convert string to byte array
        data = message.getBytes(TELNET_ENCODING);

        // max message length is 128 kB
        if (data.length > 128000) {
            LOGGER.warning("Length of message too high! Not sending");
            return false;
        }

        return send(data);
    }
}
