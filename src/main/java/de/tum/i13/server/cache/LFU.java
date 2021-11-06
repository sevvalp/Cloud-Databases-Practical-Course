package de.tum.i13.server.cache;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Class implementing an LFU cache to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class LFU implements Cache {

    private HashMap<String, String> cache;
    // to keep track of which keys were used i times
    private HashMap<Integer, LinkedList<String>> lfu_freq_key;
    // to keep track of how often each key was used
    private HashMap<String, Integer> lfu_key_freq;
    private int maxSize;
    private int currentSize;

    public LFU(int size) {
        cache = new HashMap<String, String>();
        lfu_freq_key = new HashMap<Integer, LinkedList<String>>();
        lfu_key_freq = new HashMap<String, Integer>();
        this.maxSize = size;
        this.currentSize = 0;
    }

    /**
     * Puts a key-value pair into the cache.
     *
     * @param key   Key to store.
     * @param value Value to store.
     */
    @Override
    public void put(String key, String value) {
        int f = 0;
        LinkedList<String> list = null;

        if (cache.put(key, value) == null) {
            // key not in cache
            if (++currentSize > maxSize) {
                // cache exceeding maxSize

                // find least frequently accessed keys
                int i = 1;
                while (list == null) {
                    // get list of keys, that were accessed i times
                    list = lfu_freq_key.get(i++);
                    if (list == null || list.size() == 0)
                        list = null;
                }

                // remove key from lfu and cache
                String keyToRemove = list.removeFirst();
                cache.remove(keyToRemove);
                lfu_key_freq.remove(keyToRemove);
                // do I need to do this?
                lfu_freq_key.put(--i, list);
            }
        } else {
            // key already in cache
            // remove key from old frequency
            f = lfu_key_freq.get(key);
            list = lfu_freq_key.get(f);
            list.remove(key);
            // do I need to do this?
            lfu_freq_key.put(f, list);
        }

        // add key to new frequency
        list = lfu_freq_key.get(++f);
        lfu_key_freq.put(key, f);
        if (list == null)
            list = new LinkedList<String>();
        list.add(key);
        lfu_freq_key.put(f, list);
    }

    /**
     * Deletes a key-value pair from the cache.
     *
     * @param key Key to delete.
     */
    @Override
    public void delete(String key) {
        // remove key from cache
        if (cache.remove(key) != null) {
            // remove key from key-freq map
            Integer f = lfu_key_freq.remove(key);
            LinkedList<String> list = lfu_freq_key.get(f);
            // remove key from freq-key map
            list.remove(key);
            // do I need this?
            lfu_freq_key.put(f, list);
        }
    }

    /**
     * Gets the value to a key from the cache.
     *
     * @param key Key to get
     * @return Value associated with the key from the cache
     * null, if key does not exist in cache
     */
    @Override
    public String get(String key) {
        return cache.get(key);
    }
}
