package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;

import javax.naming.SizeLimitExceededException;

import static de.tum.i13.shared.Constants.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Logger;

/**
 * Socket Communicator
 * This class is intended as a library to handle connection, interaction with a server, message passing, etc.
 *
 * @version 0.2
 * @since   2021-10-25
 *
 */
public class SocketCommunicator implements KVStore {

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
     * Inserts a key–value pair into the KVServer.
     *
     * @param key the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws IOException if there is an IOException during the put.
     * @throws IllegalStateException if currently not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    @Override
    public KVMessage put(String key, String value) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key and value to Base64
        String b64Key = Base64.getEncoder().encodeToString(key.getBytes());
        String b64Value = Base64.getEncoder().encodeToString(value.getBytes());
        String message = String.format("PUT %s %s\r\n", b64Key, b64Value);
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        send(message.getBytes(TELNET_ENCODING));
        return receiveKVMessage();
    }

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the value, which is indexed by the given key.
     * @throws IOException if there is an IOException during the get.
     * @throws IllegalStateException if currently not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    @Override
    public KVMessage get(String key) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key to Base64
        String message = String.format("GET %s\r\n", Base64.getEncoder().encodeToString(key.getBytes()));
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        send(message.getBytes(TELNET_ENCODING));
        return receiveKVMessage();
    }

    /**
     * Deletes the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the last stored value of that key
     * @throws IOException if there is an IOException during the delete.
     * @throws IllegalStateException if currently not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    public KVMessage delete(String key) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key to Base64
        String message = String.format("DELETE %s\r\n", Base64.getEncoder().encodeToString(key.getBytes()));
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        send(message.getBytes(TELNET_ENCODING));
        return receiveKVMessage();
    }

    /**
     * Sends message to the server. The message will be sanitized and finally sent using {@link #send(byte[])}.
     *
     * @param   message The message to be sent to the server
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

    /**
     * Sends data to the server, if the socket is connected. If the socket is currently not connected, the
     * method will do nothing.
     *
     * @param   data    Data to be sent to the server
     */
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
     */
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

    /**
     * Reads data from the socket using {@link #receive()} and decodes it into a String.
     *
     * @return Decoded read message.
     * @throws IOException if there is an IOException during read.
     * @throws IllegalStateException if currently not connected to a KVServer.
     */
    public String receiveMessage() throws IOException, IllegalStateException {
        String msg = new String(receive(), TELNET_ENCODING);
        return msg.substring(0, msg.length() - 2);
    }

    /**
     * Reads data from the socket using {@link #receive()} and decodes it into a KVMessage.
     *
     * @return Decoded read KVMessage.
     * @throws IOException if there is an IOExcpetion during read.
     * @throws IllegalStateException if currently not connected to a KVServer.
     */
    public KVMessage receiveKVMessage() throws IOException, IllegalStateException {
        String[] rcvMsg = receiveMessage().split("\\s");
        if (KVMessage.parseStatus(rcvMsg[0]) == null)
            return null;

        // check for errors first
        if (rcvMsg[0].equals("GET_ERROR") || rcvMsg[0].equals("PUT_ERROR") || rcvMsg[0].equals("DELETE_ERROR"))
            return new ClientMessage(KVMessage.parseStatus(rcvMsg[0]));

        // operation succeeded
        String rcvKey = new String(Base64.getDecoder().decode(rcvMsg[1]));
        String rcvVal = new String(Base64.getDecoder().decode(rcvMsg[2]));
        return new ClientMessage(rcvKey, rcvVal, KVMessage.parseStatus(rcvMsg[0]));
    }


}
