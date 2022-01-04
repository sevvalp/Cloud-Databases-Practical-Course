package de.tum.i13.server.kv;


import javax.naming.SizeLimitExceededException;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;

public class KVServerCommunicator  {

    private final static Logger LOGGER = Logger.getLogger(KVServerCommunicator.class.getName());

    private Socket mSocket;
    private OutputStream output;
    private InputStream input;
    private boolean isConnected;
    private List<String> connections;
    private TreeMap<String, OutputStream> connectionsOutputStreams;
    private TreeMap<String, InputStream> connectionsInputStreams;

    public KVServerCommunicator() {
        mSocket = new Socket();
        output = null;
        input = null;
        isConnected = false;
        connections = new ArrayList<>();
        connectionsOutputStreams = new TreeMap<>();
        connectionsInputStreams = new TreeMap<>();
    }


    public void disconnect(String key) throws Exception {
        // check if the socket is connected
        if (connections.contains(key)) {
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
            connections.remove(key);
            connectionsInputStreams.remove(key);
            connectionsOutputStreams.remove(key);
        } else {
            LOGGER.warning("Socket is not connected!");
            throw new IllegalStateException("Not connected to KVServer!");
        }

    }


    public void connect(String host, int port) throws Exception {
        // check if the socket is disconnected
        if (!connections.contains(host + ":" + port)) {
//            LOGGER.fine("Socket currently not connected, trying to connect");
            try {
                mSocket = new Socket(host, port);
//                LOGGER.info("Socket connected successfully");
                isConnected = true;
                output = mSocket.getOutputStream();
//                LOGGER.fine("Successfully got outputStream from socket");
                input = mSocket.getInputStream();
//                LOGGER.fine("Successfully got inputStream from socket");

                connections.add(host+ ":" + port);
                connectionsInputStreams.put(host+ ":" + port, input);
                connectionsOutputStreams.put(host+ ":" + port, output);

            } catch (IOException e) {
                LOGGER.severe("IO Exception while connecting, trying again");
                isConnected = false;
                e.printStackTrace();
                connect(host,port);
                //throw e;
            }
        } else {
            LOGGER.warning("Socket is currently connected! " + host + ":" + port);

            //throw new IllegalStateException("Currently connected to KVServer!");
        }

    }


    public boolean isConnected(String key) {
        return connections.contains(key);
    }


    public void send(String key, byte[] data) throws Exception {
        // check if the socket is connected
        if (connections.contains(key)) {
            if (data.length > 128000) {
                LOGGER.warning("Length of message too high! Not sending");
                throw new SizeLimitExceededException("Length of message to high!");
            }

//            LOGGER.fine("Socket currently connected, trying to send data");
            // if server is unexpectedly disconnected, we will get an exception, which we catch
            try {
                OutputStream os = connectionsOutputStreams.get(key);
                os.write(data);
                os.flush();
//                output.write(data);
//                output.flush();
                LOGGER.info("Successfully sent data");
            } catch (IOException e) {
                LOGGER.severe("IO Exception while sending data");
                connections.remove(key);
                connectionsOutputStreams.remove(key);
                connectionsInputStreams.remove(key);
                throw e;
            }
        } else {
            LOGGER.warning("Socket currently disconnected!");
            throw new IllegalStateException("Not connected to KVServer!");
        }

    }


    public byte[] receive(String key) throws Exception {
        // check if the socket is connected
        if (connections.contains(key)) {
            //LOGGER.fine("Socket currently connected, trying to read data");
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            InputStream input = connectionsInputStreams.get(key);
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
                connections.remove(key);
                connectionsInputStreams.remove(key);
                connectionsOutputStreams.remove(key);
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
