package de.tum.i13.server.processor;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class KVProcessor implements CommandProcessor {

    private static final Logger LOGGER = Logger.getLogger(KVProcessor.class.getName());

    private Cache cache;

    public KVProcessor (String... args) {
        this.cache = null;
        // TODO: handle args
    }

    @Override
    public String process(String command) {
        LOGGER.info("Received command: " + command.trim());

        // TODO: Let the magic happen here

        /* TODO:
            - add selectionKey parameter
            - split command and check for GET, PUT, DELETE --> return error message if unrecognized message
            - call respective method
         */

        return command;
    }

    /**
     * Gets the value specified by the key.
     *
     * @param command   parsed command from user.
     * @return          Stringified KVMessage answer from cache.
     */
    private String get(String[] command) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args
            - call cache.get asynchronously
            - do not return String here, but call server.send directly
         */
        return null;
    }

    /**
     * Puts the key-value pair into the store.
     *
     * @param command   parsed command from user.
     * @return          Stringified KVMessage answer from cache.
     */
    private String put(String[] command) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args
            - call cache.put asynchronously
            - do not return String here, but call server.send directly
         */
        return null;
    }

    /**
     * Deletes value from the store.
     *
     * @param command   parsed command from user.
     * @return          Stringified KVMessage answer from cache.
     */
    private String delete(String[] command) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args
            - call cache.delete asynchronously
            - do not return String here, but call server.send directly
         */
        return null;
    }

    /**
     * Converts a KVMessage to a String.
     *
     * @param msg   KVMessage to stringify.
     * @return      Stringified KVMessage.
     */
    private String stringifyKVMessage(KVMessage msg) {
        // if there is an error, there is no value
        if (msg.getStatus().name().toLowerCase().contains("error"))
            return String.format("%s %s\r\n", msg.getStatus().name().toUpperCase(), msg.getKey());
        return String.format("%s %s %s\r\n", msg.getStatus().name().toUpperCase(), msg.getKey(), msg.getValue());
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: " + remoteAddress.toString());

        return "Accepted connection from " + remoteAddress.toString() + " to KVServer ( " + address.toString() + ")\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        LOGGER.info("Connection closed: " + address.toString());
    }
}
