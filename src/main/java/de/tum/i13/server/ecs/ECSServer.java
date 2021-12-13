package de.tum.i13.server.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServerInfo;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.server.stripe.StripedCallable;
import de.tum.i13.server.stripe.StripedExecutorService;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.Util;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class ECSServer {
    private Logger LOGGER = Logger.getLogger(ECSServer.class.getName());

    private SimpleNioServer server;
    // <hash of server, server info object>
    private TreeMap<String, KVServerInfo> serverMap;
    private TreeMap<String, KVServerInfo> startingServers;
    private TreeMap<String, KVServerInfo> stoppingServers;
    private ConcurrentHashMap<String, Long> heartBeatTime;

    private ExecutorService pool;

    public ECSServer() {
        this.serverMap = new TreeMap<>();
        this.startingServers = new TreeMap<>();
        this.stoppingServers = new TreeMap<>();
        this.heartBeatTime = new ConcurrentHashMap<>();
        this.pool = new StripedExecutorService();
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

        // TODO: just for testing
        server.send(serverMap.firstEntry().getValue().getSelectionKey(), ("ECS_TEST " + B64Util.b64encode("Test") + " " + B64Util.b64encode("Value") + "\r\n").getBytes(TELNET_ENCODING));

        String message = "ecs_error " + B64Util.b64encode("error") + " " + B64Util.b64encode("unknown command") + "\r\n";
        // return answer to client
        LOGGER.info("Answer to client: " + message);
        server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
        return new ServerMessage(KVMessage.StatusType.ECS_ERROR, B64Util.b64encode("unknown"), B64Util.b64encode("command"));
    }

    public KVMessage newServer(KVMessage msg) throws UnsupportedEncodingException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        // This is a blocking execution, but we don't expect a lot of parallel communication between ECS and KVServers.
        // Hence, we implement this single threaded and don't have to deal with multithreading.

        // extract info from message
        String[] payload = B64Util.b64decode(msg.getValue()).split(",");
        if (payload.length != 3)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Cant parse payload!"));
        String address = payload[0];
        int port = Integer.parseInt(payload[1]);
        int intraPort = Integer.parseInt(payload[2]);
        LOGGER.info("New server " + address + ":" + port + " with internal port " + intraPort);

        // insert new server into Metadata
        String hash = Util.calculateHash(address, port);
        if (this.serverMap.isEmpty()) {
            LOGGER.info("Metadata is empty. Adding new server.");
            // no re-calculating, moving of data etc. necessary
            KVServerInfo info = new KVServerInfo(address, port, hash, hash, intraPort, ((ServerMessage) msg).getSelectionKey());
            String message = "update_metadata " + B64Util.b64encode(address + ":" + port) + " " + B64Util.b64encode(info.toString()) + "\r\n";
            this.serverMap.put(hash, info);
            LOGGER.fine("Server map after put: " + serverMap.toString());
            LOGGER.fine("Starting map: " + startingServers.toString());
            LOGGER.fine("Stopping map: " + stoppingServers.toString());
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
        } else {
            Map.Entry<String, KVServerInfo> prev = this.serverMap.floorEntry(hash);
            if (prev == null)
                // no prev server, check last one
                prev = this.serverMap.lastEntry();

            LOGGER.info("Previous server found. Adding new server.");
            KVServerInfo info = new KVServerInfo(address, port, hash, prev.getKey(), intraPort, ((ServerMessage) msg).getSelectionKey());
            this.startingServers.put(hash, info);
            LOGGER.fine("Starting map after put: " + startingServers.toString());
            LOGGER.fine("Server map: " + serverMap.toString());
            LOGGER.fine("Stopping map: " + stoppingServers.toString());
            String s = "";
            for (KVServerInfo i : serverMap.values())
                s += i.toString() + ";";
            s += info.toString();

            Map.Entry<String, KVServerInfo> next = this.serverMap.ceilingEntry(hash);
            if (next == null)
                next = this.serverMap.firstEntry();
            LOGGER.info("Next server found, initiate data transfer.");
            next.getValue().setStartIndex(hash);
            // send writelock to next server & transfer data to new server
            String message = "rebalance " + B64Util.b64encode(address + ":" + intraPort) + " " + B64Util.b64encode(hash) + "\r\n";
            server.send(next.getValue().getSelectionKey(), message.getBytes(TELNET_ENCODING));
        }

        return new ServerMessage(KVMessage.StatusType.ECS_ACCEPT, msg.getValue(), B64Util.b64encode("Accept connection from new server."));
    }

    public KVMessage rebalance_success(KVMessage msg) {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        // check if server just started
        String hash = B64Util.b64decode(msg.getValue());
        if (startingServers.containsKey(hash)) {
            // add to server map
            serverMap.put(hash, startingServers.remove(hash));
            sendMetadataUpdate();
        } else if (stoppingServers.containsKey(hash)) {
            stoppingServers.remove(hash);
            serverMap.remove(hash);
            sendMetadataUpdate();
        }

        return null;
    }

    public void sendMetadataUpdate() {
        if (server == null)
            return;

        pool.submit(new Runnable() {
            @Override
            public void run() {
                String s = "update_metadata " + B64Util.b64encode("update") + " ";
                for (KVServerInfo i : serverMap.values())
                    s += i.toString() + ";";
                for (KVServerInfo i : serverMap.values()) {
                    try {
                        server.send(i.getSelectionKey(), s.getBytes(TELNET_ENCODING));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public KVMessage removeServer(KVMessage msg) {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        // extract info from message
        String[] payload = B64Util.b64decode(msg.getValue()).split(",");

        // find next server
        Map.Entry next = this.serverMap.ceilingEntry(msg.getKey());
        if (next == null)
            next = this.serverMap.firstEntry();

        if (next.getKey().equals(msg.getKey())) {
            // server to remove is only KVServer connected
            // TODO: send ok to server
            this.serverMap.remove(msg.getKey());
        } else {
            // TODO: writelock on server to remove & transfer data to next server
            this.stoppingServers.put(msg.getKey(), this.serverMap.get(msg.getKey()));
        }

        return new ServerMessage(KVMessage.StatusType.ECS_ACCEPT, msg.getKey(), B64Util.b64encode("Successfully removed server"));
    }

    public KVMessage updatedMetadata(KVMessage msg) {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        // check if server is starting
        if (this.startingServers.containsKey(B64Util.b64decode(msg.getValue()))) {

        }

        return null;
    }

    public KVMessage heartbeat(KVMessage msg) {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.fine("Received heartbeat from " + B64Util.b64decode(msg.getKey()));
                long unixTimeMillis = System.currentTimeMillis();
                heartBeatTime.put(B64Util.b64decode(msg.getValue()), unixTimeMillis);
                return null;
            }

            public Object getStripe() {
                return msg.getKey();
            }
        });

        return null;
    }

    public void startHeartbeat() {
        Runnable heartBeat = new Runnable() {
            @Override
            public void run() {
                // if server is not set, we cannot send messages
                if (server == null)
                    return;

                LOGGER.info("Sending heartbeats to connected servers.");
                for (Map.Entry<String, KVServerInfo> e : serverMap.entrySet()) {
                    long unixTimeMillis = System.currentTimeMillis();

                    // check for last heartbeat
                    if (unixTimeMillis - heartBeatTime.get(e.getKey()) > 700L) {
                        LOGGER.warning("Server " + e.getValue().getAddress() + ":" + e.getValue().getPort() + "failed to respond. Removing...");
                        // TODO: remove from list
                        // TODO: update metadata
                    } else {
                        // TODO: send new heartbeat
                        String message = "ECS_HEARTBEAT " + B64Util.b64encode(e.getValue().getAddress() + ":" + e.getValue().getPort()) + " " + B64Util.b64encode(e.getKey());
                    }
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(heartBeat, 0, 1, TimeUnit.SECONDS);
    }
}
