package de.tum.i13.server.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServerCommunicator;
import de.tum.i13.server.kv.KVServerInfo;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.server.stripe.StripedCallable;
import de.tum.i13.server.stripe.StripedExecutorService;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.Util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
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

    private EcsServerCommunicator communicator;
    private ExecutorService pool;

    public ECSServer() {
        this.serverMap = new TreeMap<>();
        this.startingServers = new TreeMap<>();
        this.stoppingServers = new TreeMap<>();
        this.heartBeatTime = new ConcurrentHashMap<>();
        this.pool = new StripedExecutorService();
        this.communicator = new EcsServerCommunicator();
        this.startHeartbeat();
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
        //    sendMessage(address,port,message);
            sendMetadataUpdate();
            //server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
        } else {
            Map.Entry<String, KVServerInfo> prev = this.serverMap.floorEntry(hash);
            if (prev == null)
                // no prev server, check last one
                prev = this.serverMap.lastEntry();

            LOGGER.info("Previous server found. Adding new server.");
            KVServerInfo info = new KVServerInfo(address, port, prev.getKey(), hash, intraPort, ((ServerMessage) msg).getSelectionKey());
            //KVServerInfo info = new KVServerInfo(address, port, hash, prev.getKey(), intraPort, ((ServerMessage) msg).getSelectionKey());
            //TODO: check and update corresponding server's information (the very first one or successor node after new server added.
            // Next servers' start index should be the hash value of the prev server
            this.startingServers.put(hash, info);
            LOGGER.fine("Starting map after put: " + startingServers.toString());
            LOGGER.fine("Server map: " + serverMap.toString());
            LOGGER.fine("Stopping map: " + stoppingServers.toString());

            Map.Entry<String, KVServerInfo> next = this.serverMap.ceilingEntry(hash);
            if (next == null)
                next = this.serverMap.firstEntry();
            LOGGER.info("Next server found, initiate data transfer.");
            next.getValue().setStartIndex(hash);
            // send writelock to next server & transfer data to new server
            String message = "rebalance " + B64Util.b64encode(address + ":" + intraPort) + " " + B64Util.b64encode(hash) + "\r\n";
            sendMessage(next.getValue().getAddress(), next.getValue().getPort(), message );
            //server.send(next.getValue().getSelectionKey(), message.getBytes(TELNET_ENCODING));

            LOGGER.fine("Prev server found initiate replica data transfer");
            message = message.replace("rebalance", "replicate ");
            sendMessage(prev.getValue().getAddress(), prev.getValue().getPort(), message);
            //it is the first time we activate the replication functionality
            if(serverMap.size() + 1 == 3){
                String m = "replicate " + B64Util.b64encode(next.getValue().getAddress() + ":" + next.getValue().getIntraPort()) + " " + B64Util.b64encode(next.getKey()) + "\r\n";
                sendMessage(prev.getValue().getAddress(), prev.getValue().getPort(), m);
            }

            LOGGER.fine("Second prev server found initiate replica data transfer");
            Map.Entry<String, KVServerInfo> prevTwoTimes = this.serverMap.floorEntry(prev.getKey());
            if (prevTwoTimes == null)
                prevTwoTimes = this.serverMap.lastEntry();
            sendMessage(prevTwoTimes.getValue().getAddress(), prevTwoTimes.getValue().getPort(), message);
            //it is the first time we activate the replication functionality
            if(serverMap.size() + 1 == 3){
                String m = "replicate " + B64Util.b64encode(prev.getValue().getAddress() + ":" + prev.getValue().getIntraPort()) + " " + B64Util.b64encode(prev.getKey()) + "\r\n";
                sendMessage(prevTwoTimes.getValue().getAddress(), prevTwoTimes.getValue().getPort(), m);
            }

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

        LOGGER.info("Rebalance success received");
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
                String infos = "";
                for (KVServerInfo i : serverMap.values())
                    infos += i.toString() + ";";
                s += B64Util.b64encode(infos) + "\r\n";
                for (KVServerInfo i : serverMap.values()) {
                    try {
                        //server.send(i.getSelectionKey(), s.getBytes(TELNET_ENCODING));
                        LOGGER.info("Send metadata update to server: " + i.getAddress() + ":" + i.getPort());
                        sendMessage(i.getAddress(), i.getPort(), s);
                    } catch (Exception e) {
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
        String[] payload = B64Util.b64decode(msg.getKey()).split(",");

        // find next server
        Map.Entry<String, KVServerInfo> next = this.serverMap.ceilingEntry(msg.getKey());

        if (payload.length != 3)
            return new ServerMessage(KVMessage.StatusType.ECS_ERROR, msg.getKey(), B64Util.b64encode("Cant parse payload!"));

        String address = payload[0];
        int port = Integer.parseInt(payload[1]);
        int intraPort = Integer.parseInt(payload[2]);
        String hash = Util.calculateHash(address, port);

        if (next == null)
            next = this.serverMap.firstEntry();

        if (next.getKey().equals(msg.getKey())) {
            // server to remove is only KVServer connected
            // TODO: send ok to server
            this.serverMap.remove(msg.getKey());
        } else {
            // TODO: writelock on server to remove & transfer data to next server
            this.stoppingServers.put(hash, this.serverMap.get(hash));
//            String message = "rebalance " + B64Util.b64encode(address + ":" + intraPort) + " " + B64Util.b64encode(hash) + "\r\n";
            String message = KVMessage.StatusType.RECEIVE_REBALANCE.name().toLowerCase(Locale.ENGLISH) + " " + msg.getValue() + " " + msg.getKey() + "\r\n";
            sendMessage(next.getValue().getAddress(), next.getValue().getPort(), message );
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
        LOGGER.info("starting heartbeat");
        Runnable heartBeat = new Runnable() {
            @Override
            public void run() {
                //LOGGER.info("Sending heartbeats to connected servers.");

                // if server is not set, we cannot send messages
                if (server == null)
                    return;

                boolean disconnected = false;
                for (Map.Entry<String, KVServerInfo> e : serverMap.entrySet()) {
                    long unixTimeMillis = System.currentTimeMillis();
                    if (heartBeatTime.get(e.getKey()) == null) {
                        // KVServer never got heartbeat, initializing with current time
                        heartBeatTime.put(e.getKey(), unixTimeMillis - 500);
                        LOGGER.info("Initializing heartbeat time of server " + e.getKey());
                    }
                    // check for last heartbeat, checking for 1100ms, as there were sometimes issues using 1000ms
                    if (unixTimeMillis - heartBeatTime.get(e.getKey()) < 300L || unixTimeMillis - heartBeatTime.get(e.getKey()) >= 1100) {
                        LOGGER.warning("Server " + e.getValue().getAddress() + ":" + e.getValue().getPort() + "failed to respond. Removing...");
                        serverMap.remove(e.getKey());
                    } else {
                        LOGGER.info("Heartbeat to " + e.getValue().getAddress());
                        String message = "ECS_HEARTBEAT " + B64Util.b64encode(e.getValue().getAddress() + ":" + e.getValue().getPort()) + " " + B64Util.b64encode(e.getKey()) + "\r\n";
                        sendMessage(e.getValue().getAddress(), e.getValue().getPort(), message );
                    }
                }

                if (disconnected)
                    sendMetadataUpdate();
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(heartBeat, 0, 1, TimeUnit.SECONDS);
    }

    private void sendMessage(String address, int port, String message){

        if (!communicator.isConnected(address + ":" + port)) {
            try {
                communicator.connect(address, port);
                communicator.receive(address + ":" + port);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            communicator.send(address + ":" + port, message.getBytes(TELNET_ENCODING));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

