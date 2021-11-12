package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

/**
 * Class implementing an LFU cache to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class LFU implements Cache {

    private final static Logger LOGGER = Logger.getLogger(LFU.class.getName());

    // TODO: make this threadsafe
    private HashMap<String, String> cache;
    // to keep track of which keys were used i times
    private HashMap<Integer, LinkedList<String>> lfu_freq_key;
    // to keep track of how often each key was used
    private HashMap<String, Integer> lfu_key_freq;
    private int maxSize;
    private int currentSize;

    private static class Holder {
        private static final Cache INSTANCE = new LFU();
    }

    private LFU() {
        cache = null;
        lfu_freq_key = null;
        lfu_key_freq = null;
        this.maxSize = 0;
        this.currentSize = 0;
    }

    /**
     * Returns the cache instance.
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
        if (this.cache == null) {
            LOGGER.info(String.format("Initialized LFU cache with size %d", maxSize));
            cache = new HashMap<String, String>();
            lfu_freq_key = new HashMap<Integer, LinkedList<String>>();
            lfu_key_freq = new HashMap<String, Integer>();
            this.maxSize = maxSize;
            this.currentSize = 0;
        }
    }

    /**
     * Puts a key-value pair into the cache and optionally the disk.
     *
     * @param msg KVMessage with key and value to store.
     */
    public KVMessage put(KVMessage msg) {
        // TODO: implement semaphore
        // if cache is not yet initialized, return error
        if (cache == null)
            // we should never see this error
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Cache is not yet initialized!"));

        // if KVMessage does not have PUT command, return error
        if (msg.getStatus() != KVMessage.StatusType.PUT)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Put into cache <%s, %s>", msg.getKey(), msg.getValue()));
        int f = 0;
        LinkedList<String> list = null;

        if (cache.put(msg.getKey(), msg.getValue()) == null) {
            LOGGER.fine("Key was not in cache.");
            // key not in cache
            if (++currentSize > maxSize) {
                // cache exceeding maxSize
                LOGGER.fine("Cache exceeding max size.");

                // find least frequently accessed keys
                int i = 1;
                while (list == null) {
                    // get list of keys, that were accessed i times
                    list = lfu_freq_key.get(i++);
                    if (list == null || list.size() == 0)
                        list = null;
                }
                LOGGER.fine(String.format("Lowest frequency: %d", i-1));

                // remove key from lfu and cache
                String keyToRemove = list.removeFirst();
                cache.remove(keyToRemove);
                lfu_key_freq.remove(keyToRemove);
                // do I need to do this?
                lfu_freq_key.put(--i, list);
                LOGGER.fine(String.format("Removed key from cache: %s", keyToRemove));
            }
        } else {
            // key already in cache
            LOGGER.fine("Key already in cache.");

            // remove key from old frequency
            f = lfu_key_freq.get(msg.getKey());
            list = lfu_freq_key.get(f);
            list.remove(msg.getKey());
            // do I need to do this?
            lfu_freq_key.put(f, list);
            LOGGER.fine(String.format("Removing key from old frequency: %d", f));
        }

        // add key to new frequency
        list = lfu_freq_key.get(++f);
        lfu_key_freq.put(msg.getKey(), f);
        if (list == null)
            list = new LinkedList<String>();
        list.add(msg.getKey());
        lfu_freq_key.put(f, list);
        LOGGER.fine(String.format("Added key to new frequency: %d", f));

        return new ServerMessage(KVMessage.StatusType.PUT_SUCCESS, msg.getKey(), msg.getValue());
    }

    /**
     * Deletes a key-value pair from the cache.
     *
     * @param msg KVMessage with key to delete.
     */
    @Override
    public KVMessage delete(KVMessage msg) {
        // if cache is not yet initialized, return error
        if (cache == null)
            // we should never see this error
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Cache is not yet initialized!"));

        // if KVMessage does not have DELETE command, return error
        if (msg.getStatus() != KVMessage.StatusType.DELETE)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Removing key from cache: %s", msg.getKey()));
        // remove key from cache
        String value = cache.remove(msg.getKey());
        if (value != null) {
            // remove key from key-freq map
            Integer f = lfu_key_freq.remove(msg.getKey());
            LOGGER.fine(String.format("Removed key from lfu_key_freq with freq %d", f));
            LinkedList<String> list = lfu_freq_key.get(f);
            // remove key from freq-key map
            list.remove(msg.getKey());
            // do I need this?
            lfu_freq_key.put(f, list);
            LOGGER.fine("Removed key from lfu_freq_key");

            return new ServerMessage(KVMessage.StatusType.DELETE_SUCCESS, msg.getKey(), value);
        }

        // key not found
        return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Key not in cache!"));
    }

    /**
     * Gets the value to a key from the cache.
     *
     * @param msg KVMessage with key to get.
     * @return KVMessage with the result.
     */
    @Override
    public KVMessage get(KVMessage msg) {
        // if cache is not yet initialized, return error
        if (cache == null)
            // we should never see this error
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Cache is not yet initialized!"));

        // if KVMessage does not have GET command, return error
        if (msg.getStatus() != KVMessage.StatusType.GET)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Getting cache value for %s", msg.getKey()));
        String value = cache.get(msg.getKey());
        if (value == null) {
            LOGGER.info("Key not in cache");
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Key not in cache!"));
        }

        return new ServerMessage(KVMessage.StatusType.GET_SUCCESS, msg.getKey(), value);
    }
}
