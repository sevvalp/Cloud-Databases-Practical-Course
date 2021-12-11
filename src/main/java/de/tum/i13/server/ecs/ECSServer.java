package de.tum.i13.server.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServerInfo;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.Util;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class ECSServer {
    private Logger LOGGER = Logger.getLogger(ECSServer.class.getName());

    private SimpleNioServer server;
    // <hash of server, server info object>
    private TreeMap<String, KVServerInfo> serverMap;
    private TreeMap<String, KVServerInfo> startingServers;

    public ECSServer() {
        this.serverMap = new TreeMap<>();
        this.startingServers = new TreeMap<>();
    }

    /**
     * Sets the server to use to send messages to the client.
     *
     * @param server SimpleNioServer to use.
     */
    public void setServer(SimpleNioServer server) {
        this.server = server;
    }


    /**
     * Returns error message to client for unknown command.
     *
     * @param msg KVMessage containing unknown command.
     * @return message
     */
    public KVMessage unknownCommand(KVMessage msg) throws UnsupportedEncodingException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        String message = "ecs_error " + B64Util.b64encode("unknown command") + "\r\n";
        // return answer to client
        LOGGER.info("Answer to client: " + message);
        server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
        return new ServerMessage(KVMessage.StatusType.ECS_ERROR, B64Util.b64encode("unknown"), B64Util.b64encode("command"));
    }

    public KVMessage newServer(KVMessage msg) {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        // This is a blocking execution, but we don't expect a lot of parallel communication between ECS and KVServers.
        // Hence, we implement this single threaded and don't have to deal with multithreading.

        // extract info from message
        UUID uuid = UUID.fromString(B64Util.b64decode(msg.getKey()));
        String[] payload = B64Util.b64decode(msg.getValue()).split(",");
        if (payload.length != 3)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Cant parse payload!"));
        String address = payload[0];
        int port = Integer.parseInt(payload[1]);
        int intraPort = Integer.parseInt(payload[2]);

        // insert new server into Metadata
        String hash = Util.calculateHash(address, port);
        if (this.serverMap.isEmpty()) {
            LOGGER.info("Metadata is empty. Adding new server.");
            this.startingServers.put(hash, new KVServerInfo(uuid, address, port, hash, hash, intraPort));
            // no re-calculating, moving of data etc. necessary
            // TODO: update new server metadata
        } else {
            Map.Entry<String, KVServerInfo> prev = this.serverMap.floorEntry(hash);
            if (prev == null)
                // no prev server, check last one
                prev = this.serverMap.lastEntry();

            LOGGER.info("Previous server found. Adding new server.");
            this.startingServers.put(hash, new KVServerInfo(uuid, address, port, hash, prev.getKey(), intraPort));
            // TODO: update new server metadata

            Map.Entry next = this.serverMap.ceilingEntry(hash);
            if (next == null)
                next = this.serverMap.firstEntry();
            LOGGER.info("Next server found, initiate data transfer.");
            // TODO: writelock on next server & transfer data to new server
        }

        return new ServerMessage(KVMessage.StatusType.ECS_ACCEPT, msg.getValue(), B64Util.b64encode("Accept connection from new server."));
    }
}
