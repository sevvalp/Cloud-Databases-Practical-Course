package de.tum.i13.server.kv;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class KVCommandProcessor implements CommandProcessor {
    private static final Logger LOGGER = Logger.getLogger(KVCommandProcessor.class.getName());

    private KVStore kvStore;

    public KVCommandProcessor (KVStore kvStore) {
        this.kvStore = kvStore;
        // TODO: handle args
    }

    @Override
    public void process(String command) {
        LOGGER.info("Received command: " + command.trim());

        /* TODO:
            - add selectionKey parameter, needed to write back result to NioServer asynchronously
            - split command and check for GET, PUT, DELETE --> return error message if unrecognized message
            - call respective method
         */
    }

    /**
     * Gets the value specified by the key.
     *
     * @param command   parsed command from user.
     */
    private void get(String[] command) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args
            - call kvStore.get (include selectionKey in KVMessage!)
         */
    }

    /**
     * Puts the key-value pair into the store.
     *
     * @param command   parsed command from user.
     */
    private void put(String[] command) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args
            - call kvStore.put (include selectionKey in KVMessage!)
         */
    }

    /**
     * Deletes value from the store.
     *
     * @param command   parsed command from user.
     */
    private void delete(String[] command) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args
            - call kvStore.delete (include selectionKey in KVMessage!)
         */
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
