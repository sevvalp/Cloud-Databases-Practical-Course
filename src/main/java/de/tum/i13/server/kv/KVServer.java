package de.tum.i13.server.kv;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.server.cache.FIFO;
import de.tum.i13.server.cache.LFU;
import de.tum.i13.server.cache.LRU;
import de.tum.i13.server.disk.DiskManager;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.server.stripe.StripedCallable;
import de.tum.i13.server.stripe.StripedExecutorService;
import de.tum.i13.shared.B64Util;

import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class KVServer implements KVStore {
    private static final Logger LOGGER = Logger.getLogger(KVServer.class.getName());

    private Cache cache;
    private SimpleNioServer server;
    private ExecutorService pool;
    private DiskManager disk;

    public KVServer(String cacheType, int cacheSize) {
        if (cacheType.equals("LFU"))
            cache = LFU.getInstance();
        else if (cacheType.equals("LRU"))
            cache = LRU.getInstance();
        else
            // we default to FIFO queue if cacheType is unknown
            cache = FIFO.getInstance();

        cache.initCache(cacheSize);

        this.server = null;
        this.pool = new StripedExecutorService();
        this.disk = DiskManager.getInstance();
    }

    /**
     * Sets the server to use to send messages to the client.
     * @param server SimpleNioServer to use.
     */
    public void setServer(SimpleNioServer server) {
        this.server = server;
    }

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param msg KVMessage containing key and value to put into the store.
     * @return null
     */
    @Override
    public KVMessage put(KVMessage msg) {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
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
                if (res.getStatus() == KVMessage.StatusType.PUT_SUCCESS || res.getStatus() == KVMessage.StatusType.PUT_UPDATE) {
                    LOGGER.fine(String.format("Successfully put key into cache, now writing to disk: <%s, %s>", msg.getKey(), msg.getValue()));
                    // successfully written kv pair into cache, now write to disk
                    res = disk.writeContent(msg);
                }

                // return answer to client
                String message = res.getStatus().name() + " " + res.getKey() + " " + res.getValue() + "\r\n";
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
    public KVMessage get(KVMessage msg)  {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));
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
                KVMessage res = cache.get(msg);
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
                    }
                }

                // return answer to client
                String message = res.getStatus().name() + " " + res.getKey() + " " + res.getValue() + "\r\n";
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
                String message = res.getStatus().name() + " " + res.getKey() + " " + res.getValue() + "\r\n";
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
}
