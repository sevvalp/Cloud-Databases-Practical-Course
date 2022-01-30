package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.*;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Test Store
 * This class is intended as a library to interact with a KVCommunicator. Provide functions for the CLI.
 *
 * @version 0.1
 * @since 2021-11-09
 */
public class TestStore implements KVStore {

    private Metadata metadata;
    private final SocketCommunicator communicator;
    private static final Logger LOGGER = Logger.getLogger(TestStore.class.getName());
    public static inputPassword inputPassword = new inputPassword(false, 0);


    public TestStore() {
        communicator = new SocketCommunicator();
        metadata = new Metadata();
    }

    /**
     * Connects to the specified host on the specified port, if the socket is currently not connected. If the socket
     * is currently connected, the method does nothing.
     *
     * @param host The host to connect to.
     * @param port The port to use for the connection.
     * @return the answer from the server to the connection.
     * @throws IOException           if there is an IOException during the connection.
     * @throws IllegalStateException if currently connected to a KVServer.
     */
    public String connect(String host, int port) throws IOException, IllegalStateException {
        communicator.connect(host, port);
        String msg = new String(communicator.receive(), TELNET_ENCODING);
        return msg.substring(0, msg.length() - 2);
    }

    /**
     * Disconnects the socket, if it is currently connected. If the socket is disconnected, the method does nothing.
     *
     * @throws IOException           if there is an IOException during the disconnect.
     * @throws IllegalStateException if currently not connected to a KVServer.
     * @return a message informing about the disconnect.
     */
    public String disconnect() throws IOException, IllegalStateException {
        communicator.disconnect();
        return "Disconnected from KVServer successfully.";
    }

    /**
     * Function used for debugging purposes. Will send any command, key, value to the connected server.
     * @param command Command to send to the server.
     * @param key Key to use, will be B64 encoded.
     * @param value Value to send, will be B64 encoded.
     * @return client message
     * @throws IOException if there is an IOException during the sending.
     * @throws IllegalStateException if currently not connected to a Server.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    public String send(String command, String key, String value) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key and value to Base64
        String b64key = B64Util.b64encode(key);
        String b64Value = B64Util.b64encode(value);

        String message = String.format("%s %s %s\r\n", command.toUpperCase(), b64key, b64Value);
        LOGGER.info("Message to server: " + message);
        communicator.send(message.getBytes(TELNET_ENCODING));
        String msg = new String(communicator.receive(), TELNET_ENCODING);
        String[] split =  msg.substring(0, msg.length() - 2).split(" ");
        return split[0] + " " + B64Util.b64decode(split[1]) + " " + B64Util.b64decode(split[2]);
    }

    public String receive() throws IOException, IllegalStateException {
        String msg = new String(communicator.receive(), TELNET_ENCODING);
        String[] split =  msg.substring(0, msg.length() - 2).split(" ");
        return split[0] + " " + B64Util.b64decode(split[1]) + " " + B64Util.b64decode(split[2]);
    }

    /**
     * Will send keyrange command to the connected server.
     * @param command Command to send to the server.
     * @return client message
     * @throws IOException if there is an IOException during the sending.
     * @throws IllegalStateException if currently not connected to a Server.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    public String sendKeyRange(String command) throws IOException, IllegalStateException, SizeLimitExceededException {
        int attempts = 0;
        String message = String.format("%s \r\n", command.toUpperCase());
        LOGGER.info("Message to server: " + message);
        communicator.send(message.getBytes(TELNET_ENCODING));
        String msg = new String(communicator.receive(), TELNET_ENCODING);
        String[] response = msg.split(" ");
        while(true) {

            //Retry sending with backoff
            if(KVMessage.parseStatus(response[0]) == KVMessage.StatusType.SERVER_STOPPED) {
                try {
                    MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
                    communicator.send(message.getBytes(TELNET_ENCODING));
                    msg = new String(communicator.receive(), TELNET_ENCODING);
                    response = msg.split(" ");
                } catch (InterruptedException e) {
                    LOGGER.warning("Error while retrying to send keyrange request");
                }
            }
            else {
                return msg.substring(0, msg.length() - 2);
            }

        }
    }

    /**
     * Will send activate password protection command to the connected server.
     * @param command Command to send to the server.
     * @return client message
     * @throws IOException if there is an IOException during the sending.
     * @throws IllegalStateException if currently not connected to a Server.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    public String activatePassword(String command) throws IOException, IllegalStateException, SizeLimitExceededException {
        int attempts = 0;
        String message = String.format("%s \r\n", command.toUpperCase());
        LOGGER.info("Message to server: " + message);
        communicator.send(message.getBytes(TELNET_ENCODING));
        return "Pass activated";
    }

    /**
     * This method chooses the right server to send the query to
     * @param key the key that will be sent
     * @return the state of the operation as string
     * @throws NoSuchAlgorithmException
     * @throws NullPointerException
     * @throws IOException
     */
    private String _getCorrectServer(String key) throws NullPointerException, NoSuchAlgorithmException, IOException {

        try {
            String keyrange = sendKeyRange("keyrange ");
            metadata.updateClientMetadata(keyrange);

        } catch (SizeLimitExceededException e) {
            LOGGER.info("Exception getting keyrange while searching for the correct server.");
        }

        if(metadata != null){
            Pair<String, Integer> responsibleServer = metadata.getServerResponsible(Util.calculateHash(key));
            if(!communicator.getAddress().equals(responsibleServer.getFirst()) || communicator.getPort() != responsibleServer.getSecond()) {
                try {
                    LOGGER.info("Reconnecting to server: " + responsibleServer.getFirst() +":"+ responsibleServer.getSecond() );
                    communicator.reconnect(responsibleServer.getFirst(), responsibleServer.getSecond());
                    communicator.receive();
                    return "SUCCESS";
                } catch (IOException e) {
                    LOGGER.warning("Could not connect to responsible Server. Trying to update metadata on another server");
                    getCorrectServer(key);
                }
            } else LOGGER.warning("Could not find the responsible server. No other server exists!");
        }
        else {
            LOGGER.warning("Could not find the responsible server. No other server exists!");
        }
        return "FAIL";
    }


    public String getCorrectServer(String key){
        String returnKey = "";
        try {
            long startTime = System.nanoTime();
            returnKey = _getCorrectServer(key);
            long endTime   = System.nanoTime();
            LOGGER.info("Server swapped in (sec): " + (long) ((endTime - startTime) / 1000000000.0) );
            LOGGER.info("Server swapped in (milli sec): " + (long) ((endTime - startTime) /1000000.0) );

        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return returnKey;
    }
    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param msg KVMessage containint key and value to put into the store.″
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws IOException                if there is an IOException during the put.
     * @throws IllegalStateException      if currently not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    @Override
    public KVMessage put(KVMessage msg) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key and value to Base64
        String b64Key = B64Util.b64encode(msg.getKey());
        String b64Value = B64Util.b64encode(msg.getValue());
        String b64Pass = "";
        if(inputPassword.isInputPassword())
            b64Pass = B64Util.b64encode(msg.getPassword());
        // put message to server has the following format
        // PUT <Base64 encoded key> <Base64 encoded value>
        String message = String.format("PUT %s %s %s \r\n", b64Key, b64Value, b64Pass);
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // PUT_SUCCESS <b64 key> <b64 value>
        // PUT_ERROR <b64 key> <b64 error message>
        communicator.send(message.getBytes(TELNET_ENCODING));
        KVMessage retMsg = receiveKVMessage();
        int attempts = 0;
        while (attempts < 3) {
            if (retMsg.getStatus()== KVMessage.StatusType.SERVER_STOPPED) {
                try {
                    MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
                    communicator.send(message.getBytes(TELNET_ENCODING));
                    retMsg = receiveKVMessage();
                } catch (InterruptedException e) {
                    LOGGER.warning("Error while retrying to send put request");
                }
            }
            else {
                break;
            }
        }
        return retMsg;
    }

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param msg KVMessage containing the key to get from the store.
     * @return the value, which is indexed by the given key.
     * @throws IOException                if there is an IOException during the get.
     * @throws IllegalStateException      if currently not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    @Override
    public KVMessage get(KVMessage msg) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key to Base64
        // get message to server has the following format
        // GET <Base64 encoded key>
        String b64Pass = "";
        if(inputPassword.isInputPassword())
            b64Pass = B64Util.b64encode(msg.getPassword());
        String message = String.format("GET %s %s\r\n", B64Util.b64encode(msg.getKey()),b64Pass);
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // GET_SUCCESS <b64 key> <b64 value>
        // GET_ERROR <b64 key> <b64 error message>
        communicator.send(message.getBytes(TELNET_ENCODING));
        KVMessage retMsg = receiveKVMessage();
        int attempts = 0;
        while (attempts < 4) {
            if (retMsg.getStatus()== KVMessage.StatusType.SERVER_STOPPED) {
                try {
                    MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
                    communicator.send(message.getBytes(TELNET_ENCODING));
                    retMsg = receiveKVMessage();
                } catch (InterruptedException e) {
                    LOGGER.warning("Error while retrying to send get request");
                }
            }
            else {
                break;
            }
        }
        return retMsg;
    }

    /**
     * Deletes the value for a given key from the KVServer.
     *
     * @param msg KVMessage containint the key to delete from the store.
     * @return the last stored value of that key
     * @throws IOException                if there is an IOException during the deletion.
     * @throws IllegalStateException      if currently not connected to a KVServer.
     * @throws SizeLimitExceededException if the message is greater than 128 kB.
     */
    @Override
    public KVMessage delete(KVMessage msg) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key to Base64
        // delete message to server has the following format
        // DELETE <Base64 encoded key>
        String b64Pass = "";
        if(inputPassword.isInputPassword())
            b64Pass = B64Util.b64encode(msg.getPassword());
        String message = String.format("DELETE %s %s\r\n", B64Util.b64encode(msg.getKey()),b64Pass);
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // DELETE_SUCCESS <b64 key> <b64 value>
        // DELETE_ERROR <b64 key> <b64 error message>
        communicator.send(message.getBytes(TELNET_ENCODING));
        KVMessage retMsg = receiveKVMessage();
        int attempts = 0;
        while (attempts < 4) {
            if (retMsg.getStatus()== KVMessage.StatusType.SERVER_STOPPED) {
                try {
                    MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
                    communicator.send(message.getBytes(TELNET_ENCODING));
                    retMsg = receiveKVMessage();
                } catch (InterruptedException e) {
                    LOGGER.warning("Error while retrying to send delete request");
                }
            }
            else {
                break;
            }
        }
        return retMsg;
    }

    @Override
    public KVMessage unknownCommand(KVMessage msg) throws IOException, IllegalStateException, SizeLimitExceededException {
        // convert key to Base64
        // delete message to server has the following format
        // DELETE <Base64 encoded key>
        String message = String.format("unknown %s\r\n", B64Util.b64encode(msg.getKey()));
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // DELETE_SUCCESS <b64 key> <b64 value>
        // DELETE_ERROR <b64 key> <b64 error message>
        communicator.send(message.getBytes(TELNET_ENCODING));
        return receiveKVMessage();
    }


    /**
     * Reads data from the socket using {@link SocketCommunicator#receive()} and decodes it into a KVMessage.
     *
     * @return Decoded read KVMessage.
     * @throws IOException           if there is an IOException during read.
     * @throws IllegalStateException if currently not connected to a KVServer.
     */
    private KVMessage receiveKVMessage() throws IOException, IllegalStateException {
        String msg = new String(communicator.receive(), TELNET_ENCODING);
        String[] rcvMsg = msg.substring(0, msg.length() - 2).split("\\s");
        if (KVMessage.parseStatus(rcvMsg[0]) == null)
            return null;


        KVMessage.StatusType status = KVMessage.parseStatus(rcvMsg[0].toUpperCase(Locale.ROOT));
        if (status == KVMessage.StatusType.ERROR) {
            return null;
        }

        if (status == KVMessage.StatusType.PASSWORD_WRONG) {
            inputPassword.increaseCounter();
            return new ClientMessage(status, null, null);
        }

        if (status == KVMessage.StatusType.SERVER_WRITE_LOCK) {
            return new ClientMessage(status, null, null);
        }
        if (status == KVMessage.StatusType.SERVER_STOPPED) {
            return new ClientMessage(status, null, null);
        }
        if (status == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            return new ClientMessage(status, null, null);
        } else {
            String rcvKey = B64Util.b64decode(rcvMsg[1]);
            String rcvVal = B64Util.b64decode(rcvMsg[2]);
            return new ClientMessage(status, rcvKey, rcvVal);
        }
    }

    /**
     * This method is called to start the request of the password to the user
     */
    protected static void setInput(String[] command) {
        inputPassword.setCountPasswordInput(0);
        inputPassword.setInputPassword(true);
    }

    /**
     * This method is called to stop the request of the password to the user
     */
    protected void clearInput() {
        inputPassword.setInputPassword(false);
        inputPassword.setCountPasswordInput(0);
    }


}
