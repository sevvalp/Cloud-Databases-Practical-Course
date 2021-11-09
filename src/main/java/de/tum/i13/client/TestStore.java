package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.util.Base64;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

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
     * @throws IOException if there is an IOException during the connect.
     * @throws IllegalStateException if currently connected to a KVServer.
     */
    public String connect(String host, int port) throws IOException, IllegalStateException {
        communicator.connect(host, port);
        return communicator.receiveMessage();
    }

    /**
     * Disconnects the socket, if it is currently connected. If the socket is disconnected, the method does nothing.
     *
     * @throws IOException if there is an IOException during the disconnect.
     * @throws IllegalStateException if currently not connected to a KVServer.
     */
    public void disconnect() throws IOException, IllegalStateException {
        communicator.disconnect();
    }


    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param key   the key that identifies the given value.
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
        // put message to server has the following format
        // PUT <Base64 encoded key> <Base64 encoded value>
        String message = String.format("PUT %s %s", b64Key, b64Value);
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> (<Base64 encoded value>)
        // for example:
        // PUT_SUCCESS <b64 key> <b64 value>
        // PUT_ERROR <b64 key>
        communicator.sendMessage(message);
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
        // get message to server has the following format
        // GET <Base64 encoded key>
        String message = String.format("GET %s", Base64.getEncoder().encodeToString(key.getBytes()));
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> (<Base64 encoded value>)
        // for example:
        // GET_SUCCESS <b64 key> <b64 value>
        // GET_ERROR <b64 key>
        communicator.sendMessage(message);
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
        // delete message to server has the following format
        // DELETE <Base64 encoded key>
        String message = String.format("DELETE %s", Base64.getEncoder().encodeToString(key.getBytes()));
        LOGGER.info(String.format("Message to server: %s", message));

        // try to send data, exceptions will be rethrown
        // expected receive message
        // <STATUS> <Base64 encoded key> (<Base64 encoded value>)
        // for example:
        // DELETE_SUCCESS <b64 key> <b64 value>
        // DELETE_ERROR <b64 key>
        communicator.sendMessage(message);
        return receiveKVMessage();
    }

    /**
     * Reads data from the socket using {@link SocketCommunicator#receive()} and decodes it into a KVMessage.
     *
     * @return Decoded read KVMessage.
     * @throws IOException if there is an IOExcpetion during read.
     * @throws IllegalStateException if currently not connected to a KVServer.
     */
    public KVMessage receiveKVMessage() throws IOException, IllegalStateException {
        String[] rcvMsg = communicator.receiveMessage().split("\\s");
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
