package de.tum.i13.server.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

/**
 * Class implementing a FIFO cache to store kv-pairs. Supported operations: {@link #put(String, String)},
 * {@link #get(String)}, {@link #delete(String)}.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class FIFO implements Cache{

    private final static Logger LOGGER = Logger.getLogger(FIFO.class.getName());

    private HashMap<String, String> cache;
    private LinkedList<String> fifo;
    private int maxSize;

    public FIFO(int maxSize) {
        LOGGER.info(String.format("Created FIFO cache with size %d", maxSize));
        cache = new HashMap<String, String>();
        fifo = new LinkedList<>();
        this.maxSize = maxSize;
    }

    /**
     * Will put a kv-pair into the cache.
     *
     * @param key   Key to be stored.
     * @param value Value to be stored.
     */
    @Override
    public void put(String key, String value) {
        LOGGER.info(String.format("Put into cache: <%s, %s>", key, value));
        // insert into map
        if (cache.put(key, value) == null) {
            LOGGER.fine("Key was not in cache yet, adding...");
            // key not in fifo
            fifo.addFirst(key);

            // check if fifo is full
            if (fifo.size() > maxSize) {
                LOGGER.info("Cache full, removing last...");
                // fifo is full --> remove last element from fifo and map
                cache.remove(fifo.removeLast());
            }
        }
    }

    /**
     * Gets the value for a key from the cache.
     *
     * @param key   Key to get.
     */
    @Override
    public String get(String key) {
        LOGGER.info(String.format("Getting cache value for %s", key));
        return cache.get(key);
    }

    /**
     * Deletes a kv-pair from the cache.
     *
     * @param key   Key to delete.
     */
    @Override
    public void delete(String key) {
        LOGGER.info(String.format("Deleting key from cache: %s", key));
        cache.remove(key);
        fifo.remove(key);
    }
}
