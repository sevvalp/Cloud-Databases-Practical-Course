package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.B64Util;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

/**
 * Test Store
 * This class is intended as a library to interact with a KVCommunicator. Provide functions for the CLI.
 *
 * @version 0.1
 * @since 2021-11-09
 */
public class TestStore implements KVStore {

    private final SocketCommunicator communicator;
    private static final Logger LOGGER = Logger.getLogger(TestStore.class.getName());

    public TestStore() {
        communicator = new SocketCommunicator();
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

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // PUT_SUCCESS <b64 key> <b64 value>
        // PUT_ERROR <b64 key> <b64 error message>
        communicator.send(message.getBytes(TELNET_ENCODING));
        return receiveKVMessage();
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
        String message = String.format("GET %s\r\n", B64Util.b64encode(msg.getKey()));
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> <Base64 encoded value>
        // for example:
        // GET_SUCCESS <b64 key> <b64 value>
        // GET_ERROR <b64 key> <b64 error message>
        communicator.send(message.getBytes(TELNET_ENCODING));
        return receiveKVMessage();
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
        String message = String.format("DELETE %s\r\n", B64Util.b64encode(msg.getKey()));
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


        KVMessage.StatusType status = KVMessage.parseStatus(rcvMsg[0]);
        if(status == KVMessage.StatusType.ERROR){
            return null;
        }else{
            String rcvKey = B64Util.b64decode(rcvMsg[1]);
            String rcvVal = B64Util.b64decode(rcvMsg[2]);
            return new ClientMessage(status, rcvKey, rcvVal);
        }
    }
}
