package de.tum.i13.server.kv;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.StringJoiner;
import java.util.logging.Logger;
import java.util.logging.Level;

public class KVCommandProcessor implements CommandProcessor {
    private static final Logger LOGGER = Logger.getLogger(KVCommandProcessor.class.getName());

    private KVServer kvStore;

    public KVCommandProcessor (KVStore kvStore) {
        this.kvStore = (KVServer)kvStore;
        StartSimpleNioServer.logger.setLevel(Level.ALL);
        // TODO: handle args
        // TODO: log stuff
    }

    @Override
    public void process(SelectionKey selectionKey, String command) throws Exception {
        LOGGER.info("Received command: " + command.trim());
        String[] request = command.split("\\s");
        int size = request.length;
        request[0] = request[0].toLowerCase();

        StringJoiner v = new StringJoiner(" ");
        for (int i = 2; i < request.length; i++) {
            v.add(request[i]);
        }

        //TODO: receive write lock server, update metadata

        switch (request[0]) {
            case "put":
                kvStore.put(new ServerMessage(KVMessage.StatusType.PUT, request[1], v.toString(), selectionKey));
                LOGGER.info(String.format("Put a key with arguments: %s", String.join(" ", command)));
                break;
            case "get":
                kvStore.get(new ServerMessage(KVMessage.StatusType.GET, request[1], null, selectionKey));
                LOGGER.info(String.format("Get value with arguments: %s", String.join(" ", command)));
                break;
            case "delete":
                kvStore.delete(new ServerMessage(KVMessage.StatusType.DELETE, request[1], null, selectionKey));
                LOGGER.info(String.format("Delete a key with arguments: %s", String.join(" ", command)));
                break;
            case "keyrange":
                kvStore.getKeyRange(new ServerMessage(KVMessage.StatusType.KEY_RANGE, null, null, selectionKey));
                LOGGER.info(String.format("Get key range of the server"));
                break;
            case "rebalance":
                kvStore.rebalance(new ServerMessage(KVMessage.StatusType.REBALANCE, request[1], null, selectionKey));
                break;
            case "receive_rebalance":
                kvStore.receiveRebalance(new ServerMessage(KVMessage.StatusType.RECEIVE_REBALANCE, request[1], null, selectionKey));
                break;
            default:
                //here handle unknown commands
                kvStore.unknownCommand(new ServerMessage(KVMessage.StatusType.ERROR, "unknown", "command", selectionKey));
                LOGGER.info(String.format("Unknown command: %s", String.join(" ", command)));
                break;
        }

    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: " + remoteAddress.toString());

        return "Accepted connection from " + remoteAddress + " to KVServer ( " + address.toString() + ")\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        LOGGER.info("Connection closed: " + address.toString());
    }
}
