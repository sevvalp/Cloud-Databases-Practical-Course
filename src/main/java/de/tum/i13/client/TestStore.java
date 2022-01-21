package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServerInfo;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.*;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.TreeMap;
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
    private inputPassword inputPassword;
    private final SocketCommunicator communicator;
    private static final Logger LOGGER = Logger.getLogger(TestStore.class.getName());

    public TestStore(inputPassword inputPassword) {
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
            Pair<String, Integer> responsibleServer = metadata.getServerResponsible(keyHash(key));
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
        // put message to server has the following format
        // PUT <Base64 encoded key> <Base64 encoded value>
        String message = String.format("PUT %s %s\r\n", b64Key, b64Value);
        LOGGER.info(String.format("Message to server: %s", message));
//
//        // try to send data, exceptions will be rethrown
//        // expected receive message
//        // <STATUS> <Base64 encoded key> <Base64 encoded value>
//        // for example:
//        // PUT_SUCCESS <b64 key> <b64 value>
//        // PUT_ERROR <b64 key> <b64 error message>
//        communicator.send(message.getBytes(TELNET_ENCODING));
//        KVMessage retMsg = receiveKVMessage();
//        int attempts = 0;
//        while (attempts < 3) {
//            if (retMsg.getStatus()== KVMessage.StatusType.SERVER_STOPPED) {
//                    try {
//                        MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
//                        communicator.send(message.getBytes(TELNET_ENCODING));
//                        retMsg = receiveKVMessage();
//                    } catch (InterruptedException e) {
//                        LOGGER.warning("Error while retrying to send put request");
//                    }
//            }
//            else {
//                break;
//            }
//        }
//        return retMsg;

        //if the user input is less then 2 commands
        if (message.length() < 2) {
            //return unknown command
            System.out.println("Unknown command");
            return (new ClientMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey()));
        }
        else {
            String[] msg_arr = message.split(" ");
            String value = buildValue(msg_arr);
            ArrayList<String> request = new ArrayList<>();
            request.add(msg_arr[0]);
            request.add(msg_arr[1]);
            request.add(value);
            return handlePutRequest(msg_arr);
        }
    }

    private String buildValue(String[] command, boolean isPwd) {
        //Build the value parameter
        StringBuilder value = new StringBuilder();
        value.append(command[2]);
        for (int i = 3; i < command.length - 1; i++) {
            value.append(" ");
            value.append(command[i]);
        }
        return value.toString();
    }

    public void putKVWithPassword(String[] command, String... password) throws NoSuchAlgorithmException, SizeLimitExceededException, IOException {
        //String b64Key = B64Util.b64encode(msg.getKey());
        //String b64Value = B64Util.b64encode(msg.getValue());
        //String message = String.format("PUT %s %s\r\n", b64Key, b64Value);
        LOGGER.info(String.format("Message to server: %s", command));
        //if the user input is less then 4 commands
        if (command.length < 2) {
            //return unknown command
            System.out.println("Invalid Input");
            return;
        }
        else {
            String value = buildValue(command);
            ArrayList<String> request = new ArrayList<>();
            request.add(command[0]);
            request.add(command[1]);
            request.add(value);

            if (password.length > 0) {
                String pwd = keyHash(Arrays.toString(password));
                request.add(pwd);
                handlePutRequest(request.toArray(new String[0]));
            } else {
                handlePutRequest(request.toArray(new String[0]));
            }
        }

    }

//    @Override
//    public KVMessage get(KVMessage msg) throws Exception {
//        return null;
//    }

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
    public KVMessage get(KVMessage msg, String... password) throws IOException, IllegalStateException, SizeLimitExceededException, NoSuchAlgorithmException {
        // convert key to Base64
        // get message to server has the following format
        // GET <Base64 encoded key>
//        String message = String.format("GET %s\r\n", B64Util.b64encode(msg.getKey()));
//        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // GET_SUCCESS <b64 key> <b64 value>
        // GET_ERROR <b64 key> <b64 error message>
//        communicator.send(message.getBytes(TELNET_ENCODING));
        String pwd = keyHash(Arrays.toString(password));
//        KVMessage retMsg = receiveKVMessage();
//        int attempts = 0;
//        while (attempts < 4) {
//            if (retMsg.getStatus()== KVMessage.StatusType.SERVER_STOPPED) {
//                try {
//                    MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
//                    communicator.send(message.getBytes(TELNET_ENCODING));
//                    retMsg = receiveKVMessage();
//                } catch (InterruptedException e) {
//                    LOGGER.warning("Error while retrying to send get request");
//                }
//            }
//            else {
//                break;
//            }
//        }

        if (password.length > 0) {
            return handleMessage(new ClientMessage(msg.getStatus(), msg.getKey(), pwd));
        }
        else {
            return handleMessage(msg);
    }
//        return retMsg;
    }

    private KVMessage handleMessage (KVMessage msg) throws IOException, SizeLimitExceededException {
        String message = "";
        if (msg.getStatus() == KVMessage.StatusType.GET){
            message = String.format("GET %s\r\n", B64Util.b64encode(msg.getKey()));
        }
        else if (msg.getStatus() == KVMessage.StatusType.PUT){
            message = String.format("PUT %s\r\n", B64Util.b64encode(msg.getKey()), B64Util.b64encode(msg.getValue()));
        }
        else if (msg.getStatus() == KVMessage.StatusType.PUTPASS){
            message = String.format("PUTPASS %s\r\n", B64Util.b64encode(msg.getKey()), B64Util.b64encode(msg.getValue()));
        }
        else if (msg.getStatus() == KVMessage.StatusType.DELETE){
            message = String.format("DELETE %s\r\n", B64Util.b64encode(msg.getKey()));
        }

            LOGGER.info(String.format("Message to server: %s", message));
            communicator.send(message.getBytes(TELNET_ENCODING));
            KVMessage retMsg = receiveKVMessage();
            int attempts = 0;
            while (attempts < 4) {
                if (retMsg.getStatus() == KVMessage.StatusType.SERVER_STOPPED) {
                    try {
                        MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
                        communicator.send(message.getBytes(TELNET_ENCODING));
                        retMsg = receiveKVMessage();
                    } catch (InterruptedException e) {
                        LOGGER.warning("Error while retrying to send get request");
                    }
                } else {
                    break;
                }
        }
        return retMsg;
    }

    /**
     * This function is used to check a string is less than len bytes
     *
     * @param string the key that we want to check
     * @param len    the len we want to check
     * @return true if it is <= than len bytes else false
     */
    private boolean isLessThan(String string, int len) {
        return string.getBytes().length <= len;
    }

    /**
     * This is used to encode a plain text string to HEX String
     * We use this method do encode the value to delete the \r\n so that
     * we can send it to the server without any problem
     *
     * @param string the string that we want to encode
     * @return the encoded string
     */
    private static String encode(String string) {
        byte[] byteArray = string.getBytes();
        return byteToHex(byteArray);
    }
    private String buildValue(String[] command) {
        //Build the value parameter
        StringBuilder value = new StringBuilder();
        value.append(command[2]);
        for (int i = 3; i < command.length; i++) {
            value.append(" ");
            value.append(command[i]);
        }
        return value.toString();
    }

    /**
     * This function is used to send a put request
     *
     * @param command          the command that we want to process
     */
    private KVMessage handlePutRequest(String[] command) throws SizeLimitExceededException, IOException {
        //KVMessage put_msg = new ClientMessage(KVMessage.StatusType.PUT, command[1], command[2]);
        String value = command[2];
        if (isLessThan(value, Constants.VALUE_MAX_LENGTH) && isLessThan(command[1], Constants.KEY_MAX_LENGTH)) {
            LOGGER.info("Len: " + command.length);
            if (command.length == 4) {
                return handleMessage(new ClientMessage(KVMessage.StatusType.PUTPASS, command[1], encode(command[2]), command[3]));
            } else {
                return handleMessage(new ClientMessage(KVMessage.StatusType.PUT, command[1], encode(command[2])));
            }

            //LOGGER.info("PUT sent " + command[1] + " " + value);
        } else {
            this.inputPassword.increaseCounter();
            System.out.printf(String.format("key must be less than %s bytes, value less than %s", Constants.KEY_MAX_LENGTH, Constants.VALUE_MAX_LENGTH));
            KVMessage ret_msg = new ClientMessage(KVMessage.StatusType.PUT_ERROR, command[1], command[2]);
            return ret_msg;
        }
    }

    /**
     * This function is used to send a delete request
     *
     */
    private KVMessage handleDeleteRequest(KVMessage msg, String... pwd) throws SizeLimitExceededException, IOException, NoSuchAlgorithmException {
        String message = String.format("DELETE %s\r\n " + pwd, B64Util.b64encode(msg.getKey()));
        String[] msg_arr = message.split("");
        if (isLessThan(B64Util.b64encode(msg.getKey()), Constants.KEY_MAX_LENGTH)) {
            if (pwd.length == 0)
                return handleMessage(new ClientMessage(KVMessage.StatusType.DELETE, msg_arr[1]));
            else {
                String[] passwordList = pwd;
                String password = keyHash(Arrays.toString(passwordList));
                return handleMessage(new ClientMessage(KVMessage.StatusType.DELETE, msg_arr[1], password));
            }
            //LOGGER.fine("DELETE sent " + B64Util.b64encode(msg.getKey()));
        } else {
            System.out.println(String.format("key must be less than %s bytes", Constants.KEY_MAX_LENGTH));
            KVMessage ret_msg = new ClientMessage(KVMessage.StatusType.DELETE_ERROR, msg_arr[1]);
            return ret_msg;
        }
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
    public KVMessage delete(KVMessage msg) throws IOException, IllegalStateException, SizeLimitExceededException, NoSuchAlgorithmException {
        // convert key to Base64
        // delete message to server has the following format
        // DELETE <Base64 encoded key>
        //String message = String.format("DELETE %s\r\n", B64Util.b64encode(msg.getKey()));
        //LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // DELETE_SUCCESS <b64 key> <b64 value>
        // DELETE_ERROR <b64 key> <b64 error message>
        //communicator.send(message.getBytes(TELNET_ENCODING));
//        KVMessage retMsg = receiveKVMessage();
//        int attempts = 0;
//        while (attempts < 4) {
//            if (retMsg.getStatus()== KVMessage.StatusType.SERVER_STOPPED) {
//                try {
//                    MILLISECONDS.sleep((int) (Math.random() * Math.min(1024, Math.pow(2, attempts++))));
//                    communicator.send(message.getBytes(TELNET_ENCODING));
//                    retMsg = receiveKVMessage();
//                } catch (InterruptedException e) {
//                    LOGGER.warning("Error while retrying to send delete request");
//                }
//            }
//            else {
//                break;
//            }
//        }
//        return retMsg;
        String pwd = msg.getPassword();
        return handleDeleteRequest(msg, pwd);

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
//            String rcvVal = B64Util.b64decode(rcvMsg[2]);
            return new ClientMessage(status, rcvKey, null);
        }
    }
    /**
     * This is used to convert a byte array to hex String
     * @param in byte array to be converted
     * @return encoded string
     */
    private static String byteToHex(byte[] in) {
        StringBuilder sb = new StringBuilder();
        for(byte b : in) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * This is used to hash the key via MD5 algorithm
     * @param key string to be hashed
     * @return the hash representation
     * @throws NoSuchAlgorithmException
     */
    private String keyHash(String key) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(key.getBytes());
        byte[] byteArr = messageDigest.digest();
        messageDigest.reset();
        return byteToHex(byteArr);
    }
}
