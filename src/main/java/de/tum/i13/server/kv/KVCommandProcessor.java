package de.tum.i13.server.kv;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.logging.Logger;

public class KVCommandProcessor implements CommandProcessor {
    private static final Logger LOGGER = Logger.getLogger(KVCommandProcessor.class.getName());

    private KVStore kvStore;

    public KVCommandProcessor (KVStore kvStore) {
        this.kvStore = kvStore;
        // TODO: handle args
        // TODO: log stuff
    }

    @Override
    public void process(SelectionKey selectionKey, String command) {
        // just for testing
        // TODO: delete this
        LOGGER.info("Received command: " + command.trim());
        String[] c = command.split("\\s");
        try {
            if (c[0].equals("PUT"))
                kvStore.put(new ServerMessage(KVMessage.StatusType.PUT, c[1], c[2], selectionKey));
            else if (c[0].equals("GET"))
                kvStore.get(new ServerMessage(KVMessage.StatusType.GET, c[1], null, selectionKey));
            else if (c[0].equals("DELETE"))
                kvStore.delete(new ServerMessage(KVMessage.StatusType.DELETE, c[1], null, selectionKey));
        } catch (Exception e) {
            e.printStackTrace();
        }

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
            - check for correct number of args etc.
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
            - check for correct number of args etc.
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
            - check for correct number of args etc.
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
