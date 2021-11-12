package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

/**
 * Class implementing an LRU cache to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class LRU implements Cache{

    private final static Logger LOGGER = Logger.getLogger(LRU.class.getName());

    // TODO: make this threadsafe
    private HashMap<String, String> cache;
    private LinkedList<String> lru;
    private int maxSize;

    private static class Holder {
        private static final Cache INSTANCE = new LRU();
    }

    private LRU() {
        cache = null;
        lru = null;
        this.maxSize = 0;
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
        if (cache == null) {
            LOGGER.info(String.format("Created LRU cache with size %d", maxSize));
            cache = new HashMap<String, String>();
            lru = new LinkedList<>();
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
        // TODO: implement semaphore
        // if KVMessage does not have PUT command, return error
        if (msg.getStatus() != KVMessage.StatusType.PUT)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey());

        LOGGER.info(String.format("Put into cache: <%s, %s>", msg.getKey(), msg.getValue()));
        // insert into lru
        lru.addFirst(msg.getKey());

        // insert into map
        if (cache.put(msg.getKey(), msg.getValue()) == null && lru.size() > maxSize) {
            LOGGER.info("Cache full, removing least recently used...");
            // key not in lru and cache exceeding size --> remove last element
            cache.remove(lru.removeLast());
        } else {
            // key in lru --> remove duplicate
            lru.removeLastOccurrence(msg.getKey());
        }

        return new ServerMessage(KVMessage.StatusType.PUT_SUCCESS, msg.getKey(), msg.getValue());
    }

    /**
     * Deletes a kv-pair from the cache and the disk.
     *
     * @param msg KVMessage with key to delete.
     * @return KVMessage with the result
     */
    @Override
    public KVMessage delete(KVMessage msg) {
        // if KVMessage does not have DELETE command, return error
        if (msg.getStatus() != KVMessage.StatusType.DELETE)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey());

        LOGGER.info(String.format("Deleting key from cache: %s", msg.getKey()));
        String value = cache.remove(msg.getKey());
        lru.remove(msg.getKey());

        if (value != null)
            return new ServerMessage(KVMessage.StatusType.DELETE_SUCCESS, msg.getKey(), msg.getValue());
        return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey());
    }

    /**
     * gets the value for a key from the cache.
     *
     * @param msg KVMessage with key to get
     * @return KVMessage with the result.
     */
    @Override
    public KVMessage get(KVMessage msg) {
        // if KVMessage does not have GET command, return error
        if (msg.getStatus() != KVMessage.StatusType.GET)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey());

        LOGGER.info(String.format("Getting cache value for %s", msg.getKey()));
        String value = cache.get(msg.getKey());
        if (value == null) {
            LOGGER.info("Key not in cache, reading from disk...");
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey());
        }

        return new ServerMessage(KVMessage.StatusType.GET_SUCCESS, msg.getKey(), value);
    }
}
