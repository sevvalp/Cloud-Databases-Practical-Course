package de.tum.i13.server.kv;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.server.cache.FirstInFirstOutCache;
import de.tum.i13.server.cache.LeastFrequentlyUsedCache;
import de.tum.i13.server.cache.LeastRecentlyUsedCache;
import de.tum.i13.server.disk.DiskManager;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.server.stripe.StripedCallable;
import de.tum.i13.server.stripe.StripedExecutorService;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.Metadata;
import de.tum.i13.shared.Pair;
import de.tum.i13.shared.Util;

import java.io.*;
import java.net.InetSocketAddress;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class KVServer implements KVStore {
    private static final Logger LOGGER = Logger.getLogger(KVServer.class.getName());

    private Cache cache;
    private SimpleNioServer server;
    private ExecutorService pool;
    private DiskManager disk;

    private InetSocketAddress bootstrap;
    private String listenaddress;
    private int port;
    private int intraPort;

    private boolean serverActive;
    private boolean serverWriteLock;
    private Metadata metadata;
    private KVServerCommunicator kvServer2ServerCommunicator;
    private KVServerCommunicator kvServerECSCommunicator;
    private TreeMap<String, Pair<String, String>> historicPairs;


    public KVServer(String cacheType, int cacheSize, InetSocketAddress bootstrap, String listenaddress, int port, int intraPort) {
        if (cacheType.equals("LFU")) cache = LeastFrequentlyUsedCache.getInstance();
        else if (cacheType.equals("LRU")) cache = LeastRecentlyUsedCache.getInstance();
        else
            // we default to FIFO queue if cacheType is unknown
            cache = FirstInFirstOutCache.getInstance();

        cache.initCache(cacheSize);

        this.server = null;
        this.pool = new StripedExecutorService();
        this.disk = DiskManager.getInstance();

        this.bootstrap = bootstrap;
        this.listenaddress = listenaddress;
        this.port = port;
        this.intraPort = intraPort;
        this.kvServer2ServerCommunicator = new KVServerCommunicator();
        this.kvServerECSCommunicator = new KVServerCommunicator();
        serverActive = false;
        serverWriteLock = true;
        this.historicPairs = new TreeMap<>();

        this.metadata = new Metadata(new KVServerInfo(listenaddress,port, Util.calculateHash(listenaddress,port), "", intraPort));
        connectECS();
        addShutDownHook();

    }

    /**
     * Gets the internal port value to use to get messages from other servers and ECS.
     *
     */
    public int getIntraPort() {
        return intraPort;
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
     * Activate/Dis-activate the server to process commands.
     *
     * @param status to determine server active.
     */
    public void changeServerStatus(boolean status) {
        this.serverActive = status;
    }

    /**
     * Activate/Dis-activate the server to process put and delete commands.
     *
     * @param status to determine server write lock status.
     */
    public void changeServerWriteLockStatus(boolean status) {
        this.serverWriteLock = status;
    }

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param msg KVMessage containing key and value to put into the store.
     * @return null
     */
    @Override
    public KVMessage put(KVMessage msg) throws IOException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        //if server locked
        if (serverWriteLock) {
            String message = KVMessage.StatusType.SERVER_WRITE_LOCK.toString().toLowerCase(Locale.ENGLISH) + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_WRITE_LOCK, msg.getKey(), B64Util.b64encode("Server is locked!"));
        }
        //if server is not responsible for given key
        if(!checkServerResponsible(msg.getKey())){
            String message = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name().toLowerCase(Locale.ENGLISH) + "\r\n";
            LOGGER.info("Answer to Client: " + message);
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, metadata);
        }
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.PUT)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        LOGGER.info(String.format("Client wants to put key: <%s, %s>", msg.getKey(), msg.getValue()));

        LOGGER.fine("Submitting new put callable to pool for key " + msg.getKey());
        // queue put command
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.fine(String.format("Putting key into cache: <%s, %s>", msg.getKey(), msg.getValue()));
                // first, write the kv pair into the cache
                KVMessage res = cache.put(msg);
                String message;
//                if (res.getStatus() == KVMessage.StatusType.PUT_SUCCESS || res.getStatus() == KVMessage.StatusType.PUT_UPDATE) {
                LOGGER.fine(String.format("Successfully put key into cache, now writing to disk: <%s, %s>", msg.getKey(), msg.getValue()));
                // successfully written kv pair into cache, now write to disk
                res = disk.writeContent(msg);

                //add/update to history
                String hashedKey = Util.calculateHash(msg.getKey());
                if (!historicPairs.containsKey(hashedKey))
                    historicPairs.put(hashedKey, new Pair<>(msg.getKey(), msg.getValue()));
                else historicPairs.replace(hashedKey, new Pair<>(msg.getKey(), msg.getValue()));

//                    message = res.getStatus().name().toLowerCase() + " " + res.getKey() + " " + res.getValue() + "\r\n";
//                } else {
                message = res.getStatus().name().toLowerCase() + " " + res.getKey() + "\r\n";
//                }

                // return answer to client
                LOGGER.info("Answer to client: " + message);
                server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));

                //TODO: send new kv to replicas
                if(metadata.getServerMap().size() > 2)
                    sendKVReplicas("put", msg.getKey(), msg.getValue());

                return null;
            }


            public Object getStripe() {
                return msg.getKey();
            }
        });

        return null;
    }

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param msg KVMessage containing the key to get from the store.
     * @return null
     */
    @Override
    public KVMessage get(KVMessage msg) throws IOException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        //if server is not responsible for given key
        if(!checkServerResponsible(msg.getKey()) &&  metadata.getServerMap().size() < 3){
            String message = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name().toLowerCase(Locale.ENGLISH) + "\r\n";
            LOGGER.info("Answer to Client: " + message);
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, metadata);
        }
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.GET)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        if(metadata.isRoleReplica(msg.getKey()) || checkServerResponsible(msg.getKey())) {
            LOGGER.info("Client wants to get key: " + msg.getKey());

            LOGGER.fine("Submitting new get callable to pool for key " + msg.getKey());
            // queue get command
            pool.submit(new StripedCallable<Void>() {
                public Void call() throws Exception {
                    LOGGER.fine("Getting key from cache: " + msg.getKey());
                    // first, try to get the kv pair from the cache
                    KVMessage res = null;
                    try {
                        res = cache.get(msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    String message;
                    LOGGER.fine("Result: " + res.getStatus().name());
                    if (res.getStatus() != KVMessage.StatusType.GET_SUCCESS) {
                        LOGGER.fine("Key not in cache, try reading from disk: " + msg.getKey());
                        // key not in cache, try to read from disk
                        res = disk.readContent(msg);

                        if (res.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
                            LOGGER.fine(String.format("Successfully read key from disk, put value in cache: <%s, %s>", res.getKey(), res.getValue()));
                            // successfully got kv pair from disk, put into cache
                            // ignore result of cache put operation
                            // worst case is a new cache miss
                            cache.put(new ServerMessage(KVMessage.StatusType.PUT, res.getKey(), res.getValue()));
                            message = res.getStatus().name().toLowerCase() + " " + res.getKey() + " " + res.getValue() + "\r\n";
                        } else {
                            message = res.getStatus().name().toLowerCase() + " " + res.getKey() + " " + res.getValue() + "\r\n";
                        }

                    } else {
                        LOGGER.fine("Key in cache: " + res.getKey() + ", " + res.getValue());
                        message = res.getStatus().name().toLowerCase() + " " + res.getKey() + " " + res.getValue() + "\r\n";
                    }

                    // return answer to client

                    LOGGER.info("Answer to Client: " + message);
                    server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
                    return null;
                }

                public Object getStripe() {
                    return msg.getKey();
                }
            });

        } else {

            String message = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name().toLowerCase(Locale.ENGLISH) + "\r\n";
            LOGGER.info("Answer to Client: " + message);
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, metadata);
        }

        return null;
    }

    /**
     * Deletes the value for a given key from the KVServer.
     *
     * @param msg KVMessage containing the key to delete from the store.
     * @return null
     */
    @Override
    public KVMessage delete(KVMessage msg) throws Exception {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        //if server is not responsible for given key
        if (!checkServerResponsible(msg.getKey())) {
            String message = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name().toLowerCase(Locale.ENGLISH) + "\r\n";
            LOGGER.info("Answer to Client: " + message);
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, metadata);
        }
        //if server locked
        if (serverWriteLock) {
            String message = KVMessage.StatusType.SERVER_WRITE_LOCK.toString().toLowerCase(Locale.ENGLISH) + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_WRITE_LOCK, msg.getKey(), B64Util.b64encode("Server is locked!"));
        }
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.DELETE)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        LOGGER.info("Client wants to delete key: %s" + msg.getKey());

        LOGGER.fine("Submitting new delete callable to pool for key " + msg.getKey());
        // queue get command
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.fine("Deleting key from cache: " + msg.getKey());
                // Delete kv pair from cache
                cache.delete(msg);
                LOGGER.fine("Deleting key from disk: " + msg.getKey());
                // Delete kv pair from disk
                KVMessage res = disk.deleteContent(msg);

                //delete from history
                String hashedKey = Util.calculateHash(msg.getKey());
                historicPairs.remove(hashedKey);

                // return answer to client
                String message = res.getStatus().name().toLowerCase() + " " + res.getKey() + " " + res.getValue() + "\r\n";
                LOGGER.info("Answer to Client: " + message);
                server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));

                //TODO: delete kv from replicas
                if(metadata.getServerMap().size() > 2)
                    sendKVReplicas("delete", msg.getKey(), msg.getValue());

                return null;
            }

            public Object getStripe() {
                return msg.getKey();
            }
        });
        return null;
    }

    public void sendKVReplicas(String command, String key, String value){
        String msg = "receive_single " + command + " " + key + ":" + value + "\r\n";
        ArrayList<String> replicaServers = metadata.getReplicasHash(Util.calculateHash(listenaddress,port));

        replicaServers.forEach((server) -> {
            try {
                KVServerInfo repServer = metadata.getServerMap().get(server);
                kvServer2ServerCommunicator.connect(repServer.getAddress(), repServer.getIntraPort());
                kvServer2ServerCommunicator.receive();
                kvServer2ServerCommunicator.send(msg.getBytes(TELNET_ENCODING));
                kvServer2ServerCommunicator.disconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Returns error message to client for unknown command.
     *
     * @param msg KVMessage containing unknown command.
     * @return message
     */
    @Override
    public KVMessage unknownCommand(KVMessage msg) throws UnsupportedEncodingException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));


        String message = "error " + B64Util.b64encode("unknown command") + "\r\n";
        // return answer to client
        LOGGER.info("Answer to client: " + message);
        server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
        return new ServerMessage(KVMessage.StatusType.ERROR, B64Util.b64encode("unknown"), B64Util.b64encode("command"));

    }

    /**
     * Gets keyrange for server.
     *
     * @param msg KVMessage.
     * @return message
     */
    public KVMessage getKeyRange(KVMessage msg) throws IOException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.KEY_RANGE)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, null, B64Util.b64encode("KVMessage does not have correct status!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        LOGGER.info("Client wants to get key range");
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.fine("Calculate key range");

//                String message = KVMessage.StatusType.KEY_RANGE_SUCCESS.name().toLowerCase() + " " + metadata.getServerHashRange() + "\r\n";
                String message = metadata.getServerHashRange() + "\r\n";
                LOGGER.info("Answer to Client: " + message);

                server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
                return null;
            }

            public Object getStripe() {
                return msg.getKey();
            }
        });
        return null;

    }

    /**
     * Gets keyrange_read for server.
     *
     * @param msg KVMessage.
     * @return message
     */
    public KVMessage getKeyRangeRead(KVMessage msg) throws IOException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.KEY_RANGE_READ)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, null, B64Util.b64encode("KVMessage does not have correct status!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        LOGGER.info("Client wants to get key range success");
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {

                if( metadata.getServerMap().size() < 3)
                    getKeyRange(new ServerMessage(KVMessage.StatusType.KEY_RANGE, null, null, ((ServerMessage) msg).getSelectionKey()));
                else{
                    //TODO: implement keyrange_success method for new requirements

                    String message = metadata.getServerHashRangeWithReplicas() + "\r\n";
                    LOGGER.info("Answer to Client: " + message);

                    server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
                    return null;
                }

                return null;
            }

            public Object getStripe() {
                return msg.getKey();
            }
        });
        return null;

    }

    /**
     * Send rebalanced items to corresponding newly added server.
     *
     * @param msg KVMessage containing the new server address information.
     * @return null
     */
    public KVMessage rebalance(KVMessage msg) throws Exception {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        //if server locked
        if (serverWriteLock) {
            String message = KVMessage.StatusType.SERVER_WRITE_LOCK.toString().toLowerCase(Locale.ENGLISH) + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_WRITE_LOCK, msg.getKey(), B64Util.b64encode("Server is locked!"));
        }
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.REBALANCE)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info("Rebalance server key-value according to new server: %s" + msg.getKey());

        LOGGER.fine("Submitting new rebalance callable to pool for key " + msg.getKey());


        changeServerWriteLockStatus(true);
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.fine("Rebalancing key from cache: " + msg.getKey());

                String addressinfo[] = B64Util.b64decode(msg.getKey()).split(":");
                //get pairs which are smaller than new server hash
                String nsh = B64Util.b64decode(msg.getValue());
                String csh = Util.calculateHash(listenaddress, port);

                TreeMap<String, Pair<String, String>> sendHist = new TreeMap<>();

                for (String s : historicPairs.headMap(nsh).keySet()) {
                    sendHist.put(s, historicPairs.headMap(nsh).get(s));
                }
                historicPairs.headMap(nsh).clear();

                //get pairs which are bigger that current server hash
                for (String s : historicPairs.tailMap(csh).keySet()) {
                    sendHist.put(s, historicPairs.tailMap(csh).get(s));
                }
                historicPairs.tailMap(csh).clear();

                //delete pairs from cache and disk as well
                for (String hKey : sendHist.keySet()) {
                    String key = sendHist.get(hKey).getKey().toString();
                    LOGGER.fine("Deleting key from cache: " + key);
                    cache.delete(new ServerMessage(KVMessage.StatusType.DELETE, key, null));
                    LOGGER.fine("Deleting key from disk: " + key);
                    // Delete kv pair from disk
                    disk.deleteContent(new ServerMessage(KVMessage.StatusType.DELETE, key, null));
                }

                String message;
                if (sendHist.isEmpty() || sendHist == null) {
                    message = KVMessage.StatusType.RECEIVE_REBALANCE.name().toLowerCase(Locale.ENGLISH) + " null " + msg.getValue() + "\r\n";
                }
                else message = KVMessage.StatusType.RECEIVE_REBALANCE.name().toLowerCase(Locale.ENGLISH) + " " + B64Util.b64encode(convertMapToString(sendHist)) + " " + msg.getValue() + "\r\n";

                    LOGGER.info("Send handoff data to successor: " + message);

                    //how will I know it's selectionKey? then connect
                    kvServer2ServerCommunicator.connect(addressinfo[0], Integer.parseInt(addressinfo[1]));
                    kvServer2ServerCommunicator.receive();
                    kvServer2ServerCommunicator.send(message.getBytes(TELNET_ENCODING));
                    //LOGGER.info("Rebalance done: " + new String(kvServer2ServerCommunicator.receive(), TELNET_ENCODING));

                    //release write lock
                    //serverWriteLock = false;
                    kvServer2ServerCommunicator.disconnect();



                //kvServerECSCommunicator.connect(this.bootstrap.getAddress().getHostAddress(), this.bootstrap.getPort());
                kvServerECSCommunicator.send(("rebalance_success " + msg.getKey() + " " + msg.getValue() + "\r\n").getBytes(TELNET_ENCODING));
                LOGGER.info("Rebalance success send to ECS");

                return null;
            }
            public Object getStripe() {
                changeServerWriteLockStatus(false);
                return msg.getKey();
            }
        });

        //receive update metadata

//        String updateStr = new String(kvServerECSCommunicator.receive(), TELNET_ENCODING);
//        String[] request = updateStr.split("\\s");
//        receiveMetadata(new ServerMessage(KVMessage.StatusType.UPDATE_METADATA,request[1], request[2], null ));

        return null;
    }

    /**
     * Send KV items to corresponding newly added replicate server.
     *
     * @param msg KVMessage containing the new server address information.
     * @return null
     */
    public KVMessage replicate(KVMessage msg) throws Exception {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
        //if ECS process is not done yet, server is not ready to retrieve requests
        if (!serverActive) {
            String message = KVMessage.StatusType.SERVER_STOPPED.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_STOPPED, msg.getKey(), B64Util.b64encode("Server is not ready!"));
        }
        //if server locked
        if (serverWriteLock) {
            String message = KVMessage.StatusType.SERVER_WRITE_LOCK.toString().toLowerCase(Locale.ENGLISH) + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_WRITE_LOCK, msg.getKey(), B64Util.b64encode("Server is locked!"));
        }
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.REPLICATE)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info("Send kv items to new replicate server: %s" + msg.getKey());

        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {

                String addressinfo[] = B64Util.b64decode(msg.getKey()).split(":");

                String message;
                if (historicPairs.isEmpty() || historicPairs == null) {
                    message = KVMessage.StatusType.RECEIVE_REBALANCE.name().toLowerCase(Locale.ENGLISH) + " null " + msg.getValue() + "\r\n";
                }
                else message = KVMessage.StatusType.RECEIVE_REBALANCE.name().toLowerCase(Locale.ENGLISH) + " " + B64Util.b64encode(convertMapToString(historicPairs)) + " " + msg.getValue() + "\r\n";

                LOGGER.info("Send KV items to replicate server with this message: " + message);

                kvServer2ServerCommunicator.connect(addressinfo[0], Integer.parseInt(addressinfo[1]));
                kvServer2ServerCommunicator.receive();
                kvServer2ServerCommunicator.send(message.getBytes(TELNET_ENCODING));
                kvServer2ServerCommunicator.disconnect();
                LOGGER.info("Items successfully sent.");

                return null;
            }
            public Object getStripe() {
                changeServerWriteLockStatus(false);
                return msg.getKey();
            }
        });

        return null;
    }

    public KVMessage respondHeartbeat(KVMessage msg) {
        String message = "ecs_heartbeat " + B64Util.b64encode(listenaddress + ":" + port) + " " + B64Util.b64encode(Util.calculateHash(listenaddress, port)) + "\r\n";
        try {
            LOGGER.info("Responding to heartbeat: " + message);
            kvServerECSCommunicator.send(message.getBytes(TELNET_ENCODING));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * rebalance.
     *
     * @param msg KVMessage containing the map.
     * @return null
     */
    public KVMessage receiveRebalance(KVMessage msg) throws Exception {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));

        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.RECEIVE_REBALANCE)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info("Rebalance server key-value according to new server: %s" + msg.getKey());


        changeServerWriteLockStatus(true);
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {

                if (!msg.getKey().toString().equals("null")) {

                    LOGGER.fine("Rebalance keys from historic data" + msg.getKey());
                    String msgKey = B64Util.b64decode(msg.getKey());
                    TreeMap<String, Pair<String, String>> map = convertStringToMap(msgKey);
                    historicPairs.putAll(map);

                    for (String hKey : convertStringToMap(msgKey).keySet()) {
                        String key = historicPairs.get(hKey).getKey().toString();
                        String value = historicPairs.get(hKey).getValue().toString();
                        LOGGER.fine("Put key,value to cache: " + key + ", " + value);
                        cache.put(new ServerMessage(KVMessage.StatusType.PUT, key, value));
                        LOGGER.fine("Put key,value to disk: " + key + ", " + value);
                        disk.writeContent(new ServerMessage(KVMessage.StatusType.PUT, key, value));
                    }
                    LOGGER.fine("Rebalance done,write lock released");

                 }

                //serverWriteLock = false;
                String message = "rebalance_ok" + " " + B64Util.b64encode(listenaddress+":"+port) + " " + msg.getValue() +  "\r\n";
                //server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));

                //receive update metadata from ECS
                //        String updateStr = new String(kvServerECSCommunicator.receive(), TELNET_ENCODING);
                //        String[] request = updateStr.split("\\s");
                //        receiveMetadata(new ServerMessage(KVMessage.StatusType.UPDATE_METADATA,request[1], request[2], null ));
                return null;
            }
            public Object getStripe() {
                changeServerWriteLockStatus(false);
                return msg.getKey();
            }
        });

        return null;
    }

    /**
     * rebalance.
     *
     * @param msg KVMessage containing the map.
     * @return null
     */
    public KVMessage receiveSingleKV(KVMessage msg) throws Exception {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));

        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.RECEIVE_SINGLE)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info("Single key-value will be: " + msg.getKey() + " to/from Server.");

        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {

                String[] msgValue = B64Util.b64decode(msg.getValue()).split(":");
                String key = msgValue[0];
                String value = msgValue[1];

                if (!msg.getKey().toString().equals("put")) {

                    LOGGER.fine("Put key,value to cache: " + key + ", " + value);
                    cache.put(new ServerMessage(KVMessage.StatusType.PUT, key, value));
                    LOGGER.fine("Put key,value to disk: " + key + ", " + value);
                    disk.writeContent(new ServerMessage(KVMessage.StatusType.PUT, key, value));

                } else {
                    LOGGER.fine("Delete key,value from cache: " + key + ", " + value);
                    cache.delete(new ServerMessage(KVMessage.StatusType.DELETE, key, value));
                    LOGGER.fine("Delete key,value from disk: " + key + ", " + value);
                    disk.deleteContent(new ServerMessage(KVMessage.StatusType.DELETE, key, value));
                }
                return null;
            }
            public Object getStripe() {
                return msg.getKey();
            }
        });

        return null;
    }




    /**
     * Reads data from the socket using {@link KVCommunicator#receive()} and decodes it into Metadata.
     *
     * @throws Exception if there is an Exception during read.
     */
    public KVMessage receiveMetadata(KVMessage msg) throws Exception {

        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.REBALANCE_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));

        if (msg.getStatus() != KVMessage.StatusType.UPDATE_METADATA)
            return new ServerMessage(KVMessage.StatusType.ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        changeServerWriteLockStatus(true);

        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.info("Metadata received. ");

                String serverInfo = B64Util.b64decode(msg.getValue());
                if(serverInfo.split(";").length == 1)
                    metadata = new Metadata(new KVServerInfo(serverInfo));
                metadata.updateMetadata(serverInfo);

//                changeServerStatus(true);
//                changeServerWriteLockStatus(false);
                return null;
            }
            public Object getStripe() {
                changeServerStatus(true);
                serverWriteLock = false;
                return msg.getKey();
            }
        });

        LOGGER.info("Metadata updated, server active, write lock released");
        return null;

    }

    /**
     * Checks server is responsible for given key.
     *
     * @param key
     * @return true
     */
    public boolean checkServerResponsible(String key) {
        return metadata.checkServerResponsible(key);
    }

    /**
     * Prepare metadata map for sending client.
     *
     * @param serverMap KVMessage.
     * @return message
     */
    private String prepareMapToSend(TreeMap<String, KVServerInfo> serverMap) throws IOException {

        StringBuilder mapStr = new StringBuilder();
        for (String key : serverMap.keySet()) {
            KVServerInfo serverInfo = serverMap.get(key);
            mapStr.append(key + "_" + serverInfo.getAddress() + "_" + serverInfo.getPort() + ";");
        }
        mapStr.deleteCharAt(mapStr.length() - 1);
        return mapStr.toString() + "\r\n";

    }


    public String convertMapToString(TreeMap<String, Pair<String, String>> map) {

//        String mapAsString = "";
//        if (map != null && map.isEmpty()) {
//            mapAsString = map.keySet().stream()
//                    .map(key -> key + "=" + map.get(key).getKey() + "=" + map.get(key).getValue())
//                    .collect(Collectors.joining(", "));
//        }

        String mapAsString = "";
        for (String i : map.keySet())
            mapAsString += i.toString() + "&=&" + map.get(i).getKey().toString() + "&=&" + map.get(i).getValue().toString() + ",";
        mapAsString += mapAsString + "\r\n";

        return mapAsString;
    }

    public TreeMap<String, Pair<String, String>> convertStringToMap(String mapAsString) {
        TreeMap<String, Pair<String, String>> map = new TreeMap<>();
        mapAsString = mapAsString.replace("\r\n", "");
        mapAsString = mapAsString.substring(0, mapAsString.length() - 1);
        String[] hList = mapAsString.split(",");
        for (String data : hList) {
            String info[] = data.split("&=&");
            map.put(info[0], new Pair<>(info[1], info[2]));
        }
        return map;
    }

//    public void gracefullyShutdown() {
//        changeServerWriteLockStatus(true);
//        changeServerStatus(false);
//
//        //rebalance send all historic data to successor
//        if (metadata.getServerMap().size() > 1) {
//            try {
//                LOGGER.info("Rebalance succesor server.");
//                String svrmessage = String.format("%s %s\r\n", "receive_rebalance", B64Util.b64encode(convertMapToString(historicPairs)));
//                LOGGER.info("Message to successor: " + svrmessage);
//                //find successor
//                String sKey = metadata.getSuccessorServerKey(listenaddress, port);
//                KVServerInfo successorServer = metadata.getServerMap().get(sKey);//
//                server.send(successorServer.getSelectionKey(), svrmessage.getBytes(TELNET_ENCODING));
//                LOGGER.info("Notified successor rebalance.");
//                System.exit(0);
//
//            } catch (Exception e) {
//                LOGGER.info("Exception while stopping server.");
//            }
//        }
//
//    }

    public void connectECS(){
        LOGGER.info("Connecting to ECS");
        try {
            //connect to ECS

            kvServerECSCommunicator.connect(this.bootstrap.getAddress().getHostAddress(), this.bootstrap.getPort());
            kvServerECSCommunicator.receive();
            //notify ECS that new server added
            // NEWSERVER <encoded info: address,port,intraport>
            String command = "newserver ";
            String b64Value = B64Util.b64encode(String.format("%s,%s,%s", this.listenaddress, this.port, this.intraPort));
            String message = String.format("%s %s\r\n", command.toUpperCase(), b64Value);
            LOGGER.info("Message to server: " + message);

            LOGGER.info("Notify ECS that new server added");
            kvServerECSCommunicator.send(message.getBytes(TELNET_ENCODING));

//            String msg = new String(kvServerECSCommunicator.receive(), TELNET_ENCODING);
//            String[] request = msg.substring(0, msg.length() - 2).split("\\s");
//            kvStore.receiveMetadata(new ServerMessage(KVMessage.StatusType.UPDATE_METADATA, request[1], request[2]));

//            kvServerECSCommunicator.disconnect();

        }catch (Exception e){
            LOGGER.info("Exception while connecting ECS ");
        }
    }

    private void addShutDownHook(){
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {

               //gracefullyShutdown();
                //send ecs historic data
                LOGGER.info("Notify ECS gracefully shut down.");
                try {
                    String message = String.format("%s %s %s\r\n", "removeserver", B64Util.b64encode(String.format("%s,%s,%s", listenaddress, port, intraPort)), B64Util.b64encode(convertMapToString(historicPairs)));
//                    String message = "removeserver " + B64Util.b64encode(convertMapToString(historicPairs)) + " " + B64Util.b64encode(Util.calculateHash(listenaddress, port));
                    LOGGER.info("Message to ECS: " + message);
                    kvServerECSCommunicator.send(message.getBytes(TELNET_ENCODING));
                    LOGGER.info("Notified ECS gracefully shut down. Waiting for answer...");
                    System.out.println(kvServerECSCommunicator.receive());
                    kvServerECSCommunicator.disconnect();
                } catch (Exception e) {
                    LOGGER.info("ECS Exception while stopping server.");
                }

            }
        });
    }

}
