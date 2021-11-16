package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.MyConcurrentLinkedDeque;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

    private Map<String, CacheItem> cache;
    private MyConcurrentLinkedDeque<FrequencyItem> freqs;
    private AtomicInteger currentSize;
    private int maxSize;

    private static class Holder {
        private static final Cache INSTANCE = new LFU();
    }

    private LFU() {
        cache = null;
        freqs = null;
        this.maxSize = 0;
        this.currentSize = new AtomicInteger();
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
            cache = new ConcurrentHashMap<>();
            freqs = new MyConcurrentLinkedDeque<>();
            this.maxSize = maxSize;
        }
    }

    public KVMessage put(KVMessage msg) {
        String key = msg.getKey();
        String value = msg.getValue();
        CacheItem item = cache.get(key);
        if (item != null) {
            item.setValue(value);
            increment(item);

            return new ServerMessage(KVMessage.StatusType.PUT_UPDATE, key, value);
        } else {
            item = new CacheItem();
            item.setKey(key);
            item.setValue(value);
            cache.put(key, item);
            if (currentSize.incrementAndGet() >= maxSize)
                evict(1);
            increment(item);

            return new ServerMessage(KVMessage.StatusType.PUT_SUCCESS, key, value);
        }

    }

    public KVMessage get(KVMessage msg) {
        CacheItem item = cache.get(msg.getKey());
        if (item != null) {
            increment(item);
            return new ServerMessage(KVMessage.StatusType.GET_SUCCESS, item.getKey(), item.getValue());
        }

        return new ServerMessage(KVMessage.StatusType.GET_ERROR, item.getKey(), B64Util.b64encode("Key not in cache!"));
    }

    public KVMessage delete(KVMessage msg) {
        CacheItem item = cache.get(msg.getKey());
        if (item != null) {
            remove(item.getParentFreq(), item);
            cache.remove(msg.getKey());
            return new ServerMessage(KVMessage.StatusType.DELETE_SUCCESS, item.getKey(), item.getValue());
        }

        return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, item.getKey(), B64Util.b64encode("key not in cache!"));
    }

    public void increment(CacheItem item) {
            FrequencyItem currentFreq = item.getParentFreq();
            int nextFreqAmount;
            FrequencyItem nextFreq;

            if (currentFreq == null) {
                // item is new, set new frequency to 1
                nextFreqAmount = 1;
                    // try to get first element of frequency list
                    nextFreq = freqs.peekFirst();

            } else {
                // item already has frequency
                // calculate next freq
                nextFreqAmount = currentFreq.getFreq() + 1;
                // get next frequency node
                nextFreq = currentFreq.getNext();
            }

            if (nextFreq == null || nextFreq.getFreq() != nextFreqAmount) {
                // add new frequency item to list if it doesn't exist
                FrequencyItem newFreqItem = new FrequencyItem();
                newFreqItem.setFreq(nextFreqAmount);
                newFreqItem.setEntries(new ConcurrentLinkedDeque<>());
                newFreqItem.setNext(nextFreq);
                nextFreq = newFreqItem;
                if (currentFreq == null)
                    freqs.addFirst(newFreqItem);
                else
                    freqs.addAfter(currentFreq, newFreqItem);
            }

            // update frequency parent
            item.setParentFreq(nextFreq);
            // add item to frequency list entries
            nextFreq.getEntries().add(item);

            // remove item from old frequency list entries
            if (currentFreq != null)
                remove(currentFreq, item);
    }

    public void remove(FrequencyItem freq, CacheItem item) {
        freq.getEntries().remove(item);
        if (freq.getEntries().isEmpty())
            freqs.remove(freq);
    }

    public void evict(int count) {
        for (int i = 0; i < count;) {
            FrequencyItem f = freqs.peekFirst();
            if (f != null) {
                for (CacheItem item : f.getEntries())
                    if (i < count) {
                        cache.remove(item.getKey());
                        remove(f, item);
                        currentSize.decrementAndGet();
                        i++;
                    }
            }
        }
    }

    /**
     * Puts a key-value pair into the cache and optionally the disk.
     *
     * @pram msg KVMessage with key and value to store.
     * /
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
                // delete list if empty
                synchronized (lfu_freq_key) {
                    if (lfu_freq_key.get(lowestFreq.get()).isEmpty())
                        lfu_freq_key.remove(lowestFreq.get());
                }
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
            synchronized (lfu_freq_key) {
                if (lfu_freq_key.get(currentFreq).isEmpty()) {
                    lfu_freq_key.remove(currentFreq);
                    if (currentFreq == lowestFreq.get())
                        lowestFreq.incrementAndGet();
                }
            }

            LOGGER.fine(String.format("Removed key from old frequency: %d", currentFreq));
        }

        synchronized (lfu_freq_key) {
            // add key to new frequency
            if (lfu_freq_key.keySet().contains(++currentFreq))
                lfu_freq_key.get(currentFreq).add(key);
            else {
                Deque<String> list = new ConcurrentLinkedDeque<>();
                list.add(key);
                lfu_freq_key.put(currentFreq, list);
            }
        }
        lfu_key_freq.put(key, currentFreq);

        LOGGER.fine(String.format("Added key to new frequency: %d", currentFreq));

        if (currentFreq != 1)
            return new ServerMessage(KVMessage.StatusType.PUT_UPDATE, msg.getKey(), msg.getValue());
        return new ServerMessage(KVMessage.StatusType.PUT_SUCCESS, msg.getKey(), msg.getValue());
    }

    /**
     * Deletes a key-value pair from the cache.
     *
     * @paam msg KVMessage with key to delete.
     * /
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
            Deque<String> list = lfu_freq_key.get(f);
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
     * @paam msg KVMessage with key to get.
     * @return KVMessage with the result.
     * /
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
    */

    private class CacheItem {
        private String key;
        private String value;
        private FrequencyItem parentFreq;

        public CacheItem() {}

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public FrequencyItem getParentFreq() {
            return parentFreq;
        }

        public void setParentFreq(FrequencyItem parentFreq) {
            this.parentFreq = parentFreq;
        }

        @Override
        public String toString() {
            return String.format("<%s, %s, %d>", key, value);
        }
    }

    private class FrequencyItem {
        private Deque<CacheItem> entries;
        private int freq;
        private FrequencyItem next;

        public FrequencyItem() {}

        public Deque<CacheItem> getEntries() {
            return entries;
        }

        public void setEntries(Deque<CacheItem> entries) {
            this.entries = entries;
        }

        public int getFreq() {
            return freq;
        }

        public void setFreq(int freq) {
            this.freq = freq;
        }

        public void setNext(FrequencyItem next) {
            this.next = next;
        }

        public FrequencyItem getNext() {
            return next;
        }

        @Override
        public String toString() {
            String s = "(%s, [%s])";
            String s2 = "";
            for (CacheItem i : entries) {
                s2 = i.toString() + ", ";
            }
            s2.substring(s2.length() - 2);
            return String.format(s, freq, s2);
        }
    }
}
