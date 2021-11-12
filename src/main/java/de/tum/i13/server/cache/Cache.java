package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;

/**
 * Interface for a cache structure to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public interface Cache {

    /**
     * Puts a key-value pair into the cache.
     *
     * @param msg KVMessage with key and value to store.
     * @return KVMessage with the result.
     */
    public KVMessage put(KVMessage msg);

    /**
     * Deletes a key-value pair from the cache.
     *
     * @param msg   KVMessage with key to delete.
     * @return KVMessage with the result.
     */
    public KVMessage delete(KVMessage msg);

    /**
     * Gets the value to a key from the cache.
     *
     * @param msg   KVMessage with key to get.
     * @return KVMessage with the result.
     */
    public KVMessage get(KVMessage msg);

    /**
     * Initializes the cache data structure.
     *
     * @param maxSize the maximum number of keys to store in the cache.
     */
    public void initCache(int maxSize);

    /**
     * Returns the cache instance.
     * @return The cache instance.
     */
    public static Cache getInstance() {
        // Initialization-on-demand holder idiom
        // https://stackoverflow.com/questions/16106260/thread-safe-singleton-class
        return null;
    }
}
