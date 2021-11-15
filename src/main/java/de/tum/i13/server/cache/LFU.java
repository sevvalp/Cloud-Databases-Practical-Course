package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntBinaryOperator;
import java.util.logging.Logger;

/**
 * Class implementing an LFU cache to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class LFU implements Cache {
    // TODO: make this threadsafe
    // TODO: logging

    private final static Logger LOGGER = Logger.getLogger(LFU.class.getName());

    private HashMap<String, String> cache;
    // to keep track of which keys were used i times
    private HashMap<Integer, LinkedList<String>> lfu_freq_key;
    // to keep track of how often each key was used
    private HashMap<String, Integer> lfu_key_freq;
    private int maxSize;
    private AtomicInteger currentSize;
    private AtomicInteger lowestFreq;

    private static class Holder {
        private static final Cache INSTANCE = new LFU();
    }

    private LFU() {
        cache = null;
        lfu_freq_key = null;
        lfu_key_freq = null;
        this.maxSize = 0;
        this.currentSize = new AtomicInteger();
        this.lowestFreq = new AtomicInteger();
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
        }
    }

    /**
     * Puts a key-value pair into the cache and optionally the disk.
     *
     * @param msg KVMessage with key and value to store.
     */
    public KVMessage put(KVMessage msg) {
        // if cache is not yet initialized, return error
        if (cache == null)
            // we should never see this error
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Cache is not yet initialized!"));

        // if KVMessage does not have PUT command, return error
        if (msg.getStatus() != KVMessage.StatusType.PUT)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Put into cache <%s, %s>", msg.getKey(), msg.getValue()));
        String key = msg.getKey();
        int currentFreq = 0;

        if (cache.put(key, msg.getValue()) == null) {
            LOGGER.fine("Key was not in cache.");
            // key not in cache
            if (currentSize.getAndAccumulate(maxSize, (current, max) -> {
                if (current < max)
                    return ++current;
                return current;
            }) >= maxSize) {
                // cache exceeding maxSize
                LOGGER.fine("Cache exceeding max size.");

                // remove key from lfu and cache
                String keyToRemove = lfu_freq_key.get(lowestFreq.get()).removeFirst();
                cache.remove(keyToRemove);
                lfu_key_freq.remove(keyToRemove);
                LOGGER.fine(String.format("Removed key from cache: %s", keyToRemove));
            }
            // key not in cache --> the lowest freq is 1 (= key to put)
            this.lowestFreq.set(1);
        } else {
            // key already in cache
            LOGGER.fine("Key already in cache.");
            currentFreq = lfu_key_freq.get(key);

            // remove key from old frequency
            lfu_freq_key.get(currentFreq).remove(key);
            // update lowestFreq if list is empty and key was lowestFreq
            if (currentFreq == lowestFreq.get() && lfu_freq_key.get(currentFreq).isEmpty())
                lowestFreq.incrementAndGet();

            LOGGER.fine(String.format("Removed key from old frequency: %d", currentFreq));
        }

        // add key to new frequency
        if (lfu_freq_key.keySet().contains(++currentFreq))
            lfu_freq_key.get(currentFreq).add(key);
        else {
            LinkedList<String> list = new LinkedList<>();
            list.add(key);
            lfu_freq_key.put(currentFreq, list);
        }
        lfu_key_freq.put(key, currentFreq);

        LOGGER.fine(String.format("Added key to new frequency: %d", currentFreq));

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
