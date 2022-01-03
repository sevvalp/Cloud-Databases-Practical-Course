package de.tum.i13.server.kv;


import de.tum.i13.client.SocketCommunicator;

import javax.naming.SizeLimitExceededException;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class KVServerCommunicator implements KVCommunicator {

    private final static Logger LOGGER = Logger.getLogger(KVServerCommunicator.class.getName());

    private Socket mSocket;
    private OutputStream output;
    private InputStream input;
    private boolean isConnected;

    public KVServerCommunicator() {
        mSocket = new Socket();
        output = null;
        input = null;
        isConnected = false;
    }

    @Override
    public void disconnect() throws Exception {
        // check if the socket is connected
        if (isConnected) {
  //          LOGGER.fine("Socket currently connected, trying to disconnect.");
            try {
                // close connection
                output.close();
                input.close();
                mSocket.close();
//                LOGGER.info("Socket disconnected successfully");
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

    @Override
    public void connect(String host, int port) throws Exception {
        // check if the socket is disconnected
        if (!isConnected) {
//            LOGGER.fine("Socket currently not connected, trying to connect");
            try {
                mSocket = new Socket(host, port);
//                LOGGER.info("Socket connected successfully");
                isConnected = true;
                output = mSocket.getOutputStream();
//                LOGGER.fine("Successfully got outputStream from socket");
                input = mSocket.getInputStream();
//                LOGGER.fine("Successfully got inputStream from socket");
            } catch (IOException e) {
                LOGGER.severe("IO Exception while connecting, trying again");
                isConnected = false;
                e.printStackTrace();
                connect(host,port);
                //throw e;
            }
        } else {
            LOGGER.warning("Socket is currently connected!");
            //throw new IllegalStateException("Currently connected to KVServer!");
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


    @Override
    public void send(byte[] data) throws Exception {
        // check if the socket is connected
        if (isConnected) {
            if (data.length > 128000) {
                LOGGER.warning("Length of message too high! Not sending");
                throw new SizeLimitExceededException("Length of message too high!");
            }

//            LOGGER.fine("Socket currently connected, trying to send data");
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

    @Override
    public byte[] receive() throws Exception {
        // check if the socket is connected
        if (isConnected) {
            //LOGGER.fine("Socket currently connected, trying to read data");
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

    public byte[] receive(InputStream input) throws Exception {
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
}
