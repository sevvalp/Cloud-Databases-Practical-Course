package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Class implementing an LRU cache to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class LeastRecentlyUsedCache implements Cache{

    private final static Logger LOGGER = Logger.getLogger(LeastRecentlyUsedCache.class.getName());

    private Map<String, String> cache;
    private Deque<String> lru;
    private int maxSize;
    private AtomicInteger currentSize;

    private static class Holder {
        private static final Cache INSTANCE = new LeastRecentlyUsedCache();
    }

    private LeastRecentlyUsedCache() {
        cache = new ConcurrentHashMap<>();
        lru = new ConcurrentLinkedDeque<>();
        this.maxSize = 0;
        currentSize = new AtomicInteger();
    }

    /**
     * Returns the cache instance.
     *
     * @return The cache instance.
     */
    public static Cache getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Initializes the cache data structure.
     *
     * @param maxSize the maximum number of keys to store in the cache.
     */
    public void initCache(int maxSize) {
        // only init if cache is null
        if (maxSize < 0) {
            LOGGER.info(String.format("Created LRU cache with size %d", maxSize));
            this.maxSize = maxSize;
        }
    }

    /**
     * Will put a kv-pair into the cache and the disk.
     *
     * @param msg KVMessage with key and value to store.
     * @return KVMessage with the result.
     */
    @Override
    public KVMessage put(KVMessage msg) {
        // if cache is not yet initialized, return error
        if (maxSize < 0)
            // we should never see this error
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Cache is not yet initialized!"));

        // if KVMessage does not have PUT command, return error
        if (msg.getStatus() != KVMessage.StatusType.PUT)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Put into cache: <%s, %s>", msg.getKey(), msg.getValue()));
        LOGGER.finer("LRU before put: " + lru);
        // insert into lru
        lru.add(msg.getKey());

        // insert into map
        if (cache.put(msg.getKey(), msg.getValue()) == null) {
            if (currentSize.getAndAccumulate(maxSize, (current, max) -> {
                if (current < max)
                    return ++current;
                return current;
            }) >= maxSize) {
                LOGGER.info("Cache full, removing least recently used...");
                // key not in lru and cache exceeding size --> remove last element
                cache.remove(lru.remove());
            }
            LOGGER.finer("LRU after put: " + lru);
            return new ServerMessage(KVMessage.StatusType.PUT_SUCCESS, msg.getKey(), msg.getValue());
        }

        LOGGER.finer("Key already in cache, removing duplicate: " + msg.getKey());
        // key in lru --> remove duplicate
        lru.remove(msg.getKey());

        LOGGER.finer("LRU after put: " + lru);
        return new ServerMessage(KVMessage.StatusType.PUT_UPDATE, msg.getKey(), msg.getValue());
    }

    /**
     * Deletes a kv-pair from the cache and the disk.
     *
     * @param msg KVMessage with key to delete.
     * @return KVMessage with the result
     */
    @Override
    public KVMessage delete(KVMessage msg) {
        // if cache is not yet initialized, return error
        if (maxSize < 0)
            // we should never see this error
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Cache is not yet initialized!"));

        // if KVMessage does not have DELETE command, return error
        if (msg.getStatus() != KVMessage.StatusType.DELETE)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Deleting key from cache: %s", msg.getKey()));
        LOGGER.finer("LRU before delete: " + lru);
        String value = cache.remove(msg.getKey());
        lru.remove(msg.getKey());

        LOGGER.finer("LRU after delete: " + lru);
        if (value != null) {
            currentSize.decrementAndGet();
            return new ServerMessage(KVMessage.StatusType.DELETE_SUCCESS, msg.getKey(), msg.getValue());
        }
        return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Key not in cache!"));
    }

    /**
     * gets the value for a key from the cache.
     *
     * @param msg KVMessage with key to get
     * @return KVMessage with the result.
     */
    @Override
    public KVMessage get(KVMessage msg) {
        // if cache is not yet initialized, return error
        if (maxSize < 0)
            // we should never see this error
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Cache is not yet initialized!"));

        // if KVMessage does not have GET command, return error
        if (msg.getStatus() != KVMessage.StatusType.GET)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Getting cache value for %s", msg.getKey()));
        LOGGER.finer("LRU before get: " + lru);
        String value = cache.get(msg.getKey());

        if (value == null) {
            LOGGER.info("Key not in cache");
            LOGGER.finer("LRU after get: " + lru);
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Key not in cache!"));
        }

        // insert at tail of lru
        lru.add(msg.getKey());
        // delete old entry
        lru.remove(msg.getKey());
        LOGGER.finer("LRU after get: " + lru);

        return new ServerMessage(KVMessage.StatusType.GET_SUCCESS, msg.getKey(), value);
    }
}
