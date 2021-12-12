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

import java.io.*;
import java.net.InetSocketAddress;



import java.util.TreeMap;
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
    private KVServerCommunicator kvServerECSCommunicator;


    //TODO: add shutdown hook
    //TODO: Hands off data items (keep track of put, delete files)


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
        kvServerECSCommunicator = new KVServerCommunicator();
        serverActive = false;
        serverWriteLock = true;

        KVServerInfo serverInfo = new KVServerInfo(this.listenaddress, this.port,metadata.calculateHash(this.listenaddress+this.port),
                metadata.calculateHash(this.listenaddress+this.port),this.intraPort);
        this.metadata = new Metadata(serverInfo);
        TreeMap<String, KVServerInfo> serverMap = new TreeMap<>();
        serverMap.put(metadata.calculateHash(this.listenaddress+this.port), serverInfo);
        this.metadata.setServerMap(serverMap);

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
            String message = KVMessage.StatusType.SERVER_WRITE_LOCK.toString().toLowerCase() + "\r\n";
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_WRITE_LOCK, msg.getKey(), B64Util.b64encode("Server is locked!"));
        }
        //if server is not responsible for given key
        if(!checkServerResponsible(msg.getKey())){
            String message = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name().toLowerCase() + " " + prepareMapToSend(metadata.getServerMap());
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
                if (res.getStatus() == KVMessage.StatusType.PUT_SUCCESS || res.getStatus() == KVMessage.StatusType.PUT_UPDATE) {
                    LOGGER.fine(String.format("Successfully put key into cache, now writing to disk: <%s, %s>", msg.getKey(), msg.getValue()));
                    // successfully written kv pair into cache, now write to disk
                    res = disk.writeContent(msg);
                    message = res.getStatus().name().toLowerCase() + " " + res.getKey() + " " + res.getValue() + "\r\n";
                } else {
                    message = res.getStatus().name().toLowerCase() + " " + res.getKey() + "\r\n";
                }

                // return answer to client
                LOGGER.info("Answer to client: " + message);
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
        if(!checkServerResponsible(msg.getKey())){
            String message = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name().toLowerCase() + " " + prepareMapToSend(metadata.getServerMap());
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
        if(!checkServerResponsible(msg.getKey())){
            String message = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name().toLowerCase() + " " + prepareMapToSend(metadata.getServerMap());
            LOGGER.info("Answer to Client: " + message);
            server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
            return new ServerMessage(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, metadata);
        }
        //if server locked
        if (serverWriteLock) {
            String message = KVMessage.StatusType.SERVER_WRITE_LOCK.toString().toLowerCase() + "\r\n";
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

                // return answer to client
                String message = res.getStatus().name().toLowerCase() + " " + res.getKey() + " " + res.getValue() + "\r\n";
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
        //if server is not responsible for given key
        // if KVMessage does not have put command, return error
        if (msg.getStatus() != KVMessage.StatusType.KEY_RANGE)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, null, B64Util.b64encode("KVMessage does not have correct status!"));
        // if KVMessage does not contain selectionKey, return error
        if (!(msg instanceof ServerMessage) || ((ServerMessage) msg).getSelectionKey() == null)
            return new ServerMessage(KVMessage.StatusType.KEY_RANGE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not contain selectionKey!"));

        LOGGER.info("Client wants to get key range");
        LOGGER.fine("Calculate key range");

        String message = KVMessage.StatusType.KEY_RANGE_SUCCESS + " " + metadata.getServerHashRange();
        LOGGER.info("Answer to Client: " + message);

        server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));
        return null;

    }


    /**
     * Connects ECS Service.
     *
     *
     */
    public void connectECS(){
        LOGGER.info("Connecting to ECS");
        try {
            //connect to ECS
            kvServerECSCommunicator.connect(this.bootstrap.getHostName(), this.bootstrap.getPort());
            //notify ECS that new server added
            String command = "newserver" + this.listenaddress + this.port + this.intraPort;
            LOGGER.info("Notify ECS that new server added");
            kvServerECSCommunicator.send(command.getBytes(TELNET_ENCODING));
            //receive ECS response: new ServerMessage(KVMessage.StatusType.SERVER_READY, metadata)
            byte[] data = kvServerECSCommunicator.receive();
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
            ServerMessage msg = (ServerMessage) in.readObject();


            if(msg.getStatus().equals(KVMessage.StatusType.SERVER_READY)){
                LOGGER.info("Metadata received");
                this.metadata = msg.getMetadata();
                changeServerStatus(true);
                changeServerWriteLockStatus(false);
            }else{
                LOGGER.info("Metadata could not received");
            }

            in.close();

        }catch (Exception e){
            LOGGER.info("Exception while connecting ECS ");
        }

    }

    /**
     * Checks server is responsible for given key.
     *
     * @param key
     * @return true
     */
    public boolean checkServerResponsible(String key) {
        return metadata.checkServerResposible(key);
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
        mapStr.deleteCharAt(mapStr.length()-1);
        return mapStr.toString() + "\r\n";

    }

}
