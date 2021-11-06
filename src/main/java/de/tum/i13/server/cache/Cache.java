package de.tum.i13.server.cache;

public interface Cache {

    /**
     * Puts a key-value pair into the cache.
     *
     * @param key   Key to store.
     * @param value Value to store.
     */
    public void put(String key, String value);

    /**
     * Deletes a key-value pair from the cache.
     *
     * @param key   Key to delete.
     */
    public void delete(String key);

    /**
     * Gets the value to a key from the cache.
     *
     * @param key   Key to get
     * @return      Value associated with the key from the cache
     *              null, if key does not exist in cache
     */
    public String get(String key);
}
