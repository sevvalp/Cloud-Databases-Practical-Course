package de.tum.i13.server.kv;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.server.cache.FIFO;
import de.tum.i13.server.cache.LFU;
import de.tum.i13.server.cache.LRU;

public class KVServer implements KVStore {

    private Cache cache;

    public KVServer(String cacheType) {
        if (cacheType.equals("LFU"))
            cache = LFU.getInstance();
        else if (cacheType.equals("LRU"))
            cache = LRU.getInstance();
        else
            // we default to FIFO queue if cacheType is unknown
            cache = FIFO.getInstance();
    }

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param msg KVMessage containing key and value to put into the store.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    @Override
    public KVMessage put(KVMessage msg) throws Exception {
        return null;
    }

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param msg KVMessage containing the key to get from the store.
     * @return the value, which is indexed by the given key.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    @Override
    public KVMessage get(KVMessage msg) throws Exception {
        return null;
    }

    /**
     * Deletes the value for a given key from the KVServer.
     *
     * @param msg KVMessage containing the key to delete from the store.
     * @return the last stored value of that key
     * @throws Exception if delete command cannot be executed (e.g. not connected to any KV server).
     */
    @Override
    public KVMessage delete(KVMessage msg) throws Exception {
        return null;
    }
}
