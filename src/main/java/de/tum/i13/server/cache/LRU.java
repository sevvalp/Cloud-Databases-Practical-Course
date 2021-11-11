package de.tum.i13.server.cache;

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

    private HashMap<String, String> cache;
    private LinkedList<String> lru;
    private int maxSize;

    public LRU(int maxSize) {
        LOGGER.info(String.format("Created LRU cache with size %d", maxSize));
        cache = new HashMap<String, String>();
        lru = new LinkedList<>();
        this.maxSize = maxSize;
    }

    /**
     * Will put a kv-pair into the cache and the disk.
     *
     * @param key   Key to store.
     * @param value Value to store.
     */
    @Override
    public void put(String key, String value) {
        this.put(key, value, true);
    }

    /**
     * Will put a kv-pair into the cache and optionally the disk.
     *
     * @param key           Key to store.
     * @param value         Value to store.
     * @param writeToDisk   Flag if value should be written to the disk.
     */
    public void put(String key, String value, boolean writeToDisk) {
        LOGGER.info(String.format("Put into cache: <%s, %s>", key, value));
        // insert into lru
        lru.addFirst(key);

        // insert into map
        if (cache.put(key, value) == null && lru.size() > maxSize) {
            LOGGER.info("Cache full, removing least recently used...");
            // key not in lru and cache exceeding size --> remove last element
            cache.remove(lru.removeLast());
        } else {
            // key in lru --> remove duplicate
            lru.removeLastOccurrence(key);
        }

        if (writeToDisk) {
            LOGGER.fine("Writing value to disk...");
            // TODO: asynchronously write to disk
        }
    }

    /**
     * Deletes a kv-pair from the cache and the disk.
     *
     * @param key   Key to delete.
     */
    @Override
    public void delete(String key) {
        LOGGER.info(String.format("Deleting key from cache: %s", key));
        cache.remove(key);
        lru.remove(key);
        // TODO: asynchronously delete from disk
    }

    /**
     * gets the value for a key from the cache.
     *
     * @param key   Key to get
     * @return
     */
    @Override
    public String get(String key) {
        LOGGER.info(String.format("Getting cache value for %s", key));
        String value = cache.get(key);
        if (value == null) {
            LOGGER.info("Key not in cache, reading from disk...");
            // TODO: read from disk
            value = "TODO: read from disk";
            this.put(key, value, false);
        }

        return value;
    }
}
