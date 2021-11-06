package de.tum.i13.server.cache;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Class implementing an LRU cache to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class LRU implements Cache{

    private HashMap<String, String> cache;
    private LinkedList<String> lru;
    private int maxSize;

    public LRU(int maxSize) {
        cache = new HashMap<String, String>();
        lru = new LinkedList<>();
        this.maxSize = maxSize;
    }

    /**
     * Will put a kv-pair into the cache.
     *
     * @param key   Key to store.
     * @param value Value to store.
     */
    @Override
    public void put(String key, String value) {
        // insert into lru
        lru.addFirst(key);

        // insert into map
        if (cache.put(key, value) == null && lru.size() > maxSize) {
            // key not in lru and cache exceeding size --> remove last element
            cache.remove(lru.removeLast());
        } else {
            // key in lru --> remove duplicate
            lru.removeLastOccurrence(key);
        }
    }

    /**
     * Deletes a kv-pair from the cache.
     *
     * @param key   Key to delete.
     */
    @Override
    public void delete(String key) {
        cache.remove(key);
        lru.remove(key);
    }

    /**
     * gets the value for a key from the cache.
     *
     * @param key   Key to get
     * @return
     */
    @Override
    public String get(String key) {
        return cache.get(key);
    }
}
