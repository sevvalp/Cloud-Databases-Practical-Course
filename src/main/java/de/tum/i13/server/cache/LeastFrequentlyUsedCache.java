package de.tum.i13.server.cache;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Class implementing an LFU cache to store kv-pairs.
 * Loosely based on
 *      https://ieftimov.com/post/when-why-least-frequently-used-cache-implementation-golang/
 *      http://dhruvbird.com/lfu.pdf
 *
 *      https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Executor.html
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class LeastFrequentlyUsedCache implements Cache {
    private final static Logger LOGGER = Logger.getLogger(LeastFrequentlyUsedCache.class.getName());

    private Map<String, CacheItem> cache;
    // store which items are on which frequency level
    // only used in increment/remove, which run in series --> no concurrent list needed
    private LinkedList<FrequencyItem> freqs;
    // only used in increment/evict, which run in series --> no AtomicInteger needed
    private int currentSize;
    private int maxSize;
    private Executor executor;

    private static class Holder {
        private static final Cache INSTANCE = new LeastFrequentlyUsedCache();
    }

    private LeastFrequentlyUsedCache() {
        cache = new ConcurrentHashMap<>();
        freqs = new LinkedList<>();
        this.maxSize = -1;
        this.currentSize = 0;
        this.executor = new SerialExecutor(Executors.newFixedThreadPool(1));
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
        // only init if cache is not yet initialized
        if (this.maxSize < 0) {
            LOGGER.info(String.format("Initialized LFU cache with size %d", maxSize));
            this.maxSize = maxSize;
        }
    }

    /**
     * Puts a key-value pair into the cache.
     *
     * @param msg KVMessage with key and value to store.
     */
    public KVMessage put(KVMessage msg) {
        String key = msg.getKey();
        String value = msg.getValue();
        CacheItem item = cache.get(key);
        LOGGER.info("Putting into cache: <" + key + ", " + value + ">.");

        if (item != null) {
            // lock item so other get requests have to wait
            item.getLock().writeLock().lock();
            LOGGER.fine("Key " + key + " in cache, set new value.");
            try {
                item.setValue(value);
            } finally {
                item.getLock().writeLock().unlock();
            }
            increment(item);

            return new ServerMessage(KVMessage.StatusType.PUT_UPDATE, key, value);
        } else {
            LOGGER.fine("Key " + key + " not in cache. Creating new CacheItem.");
            item = new CacheItem(key, value);
            // lock item so other get requests have to wait
            item.getLock().writeLock().lock();
            try {
                cache.put(key, item);
            } finally {
                item.getLock().writeLock().unlock();
            }
            // increment of currentSize in increment method
            increment(item);

            return new ServerMessage(KVMessage.StatusType.PUT_SUCCESS, key, value);
        }

    }

    /**
     * Gets the value to a key from the cache.
     *
     * @param msg KVMessage with key to get.
     * @return KVMessage with the result.
     */
    public KVMessage get(KVMessage msg) {
        String key = msg.getKey();
        CacheItem item = cache.get(key);
        LOGGER.info("Getting from cache: " + key);

        if (item != null) {
            // lock item to get current value
            item.getLock().readLock().lock();
            LOGGER.fine("Key " + key + " in cache, reading value.");
            String value;
            try {
                value = item.getValue();
            } finally {
                item.getLock().readLock().unlock();
            }
            increment(item);
            return new ServerMessage(KVMessage.StatusType.GET_SUCCESS, key, value);
        }

        LOGGER.fine("Key " + key + " not in cache!");
        return new ServerMessage(KVMessage.StatusType.GET_ERROR, key, B64Util.b64encode("Key not in cache!"));
    }

    /**
     * Deletes a key-value pair from the cache.
     *
     * @param msg KVMessage with key to delete.
     */
    public KVMessage delete(KVMessage msg) {
        String key = msg.getKey();
        CacheItem item = cache.get(key);
        LOGGER.info("Deleting from cache: " + key);

        if (item != null) {
            LOGGER.fine("Key " + key + " in cache, deleting item.");
            cache.remove(key);
            removeAsync(item);
            return new ServerMessage(KVMessage.StatusType.DELETE_SUCCESS, item.getKey(), item.getValue());
        }

        LOGGER.fine("Key " + key + " not in cache!");
        return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, key, B64Util.b64encode("key not in cache!"));
    }

    public void increment(CacheItem item) {
        // let SerialExecutor update the increment asynchronously
        executor.execute(new Runnable() {
            @Override
            public void run() {
                FrequencyItem currentFreq = item.getParentFreq();
                int nextFreqAmount;
                FrequencyItem nextFreq;
                LOGGER.info("Incrementing key " + item.getKey());
                String log = "LFU before increment: ";
                for (FrequencyItem i : freqs)
                    log += i.toString() + " ";
                LOGGER.fine(log);

                if (currentFreq == null) {
                    // new item added to cache --> increment size and check for eviction
                    if (++currentSize > maxSize) {
                        // cache exceeding maxSize
                        LOGGER.fine("Cache exceeding max size.");
                        evict(1);
                    }

                    nextFreqAmount = 1;
                    nextFreq = freqs.peekFirst();
                } else {
                    if (!currentFreq.getEntries().contains(item)) {
                        LOGGER.info("Item was deleted or evicted before!");
                        return;
                    }

                    // item already in cache before, calculate next frequency
                    nextFreqAmount = currentFreq.getFreq() + 1;
                    try {
                        nextFreq = freqs.get(freqs.indexOf(currentFreq) + 1);
                    } catch (IndexOutOfBoundsException e) {
                        nextFreq = null;
                    }
                }

                if (nextFreq == null || nextFreq.getFreq() != nextFreqAmount) {
                    LOGGER.fine("Next freq amount doesn't exist.");
                    nextFreq = new FrequencyItem(nextFreqAmount);

                    if (currentFreq == null) {
                        // new item -> nextFreqAmount = 1
                        // correct freq didn't exist --> push new freq to front of list
                        freqs.addFirst(nextFreq);
                    } else {
                        freqs.add(freqs.indexOf(currentFreq) + 1, nextFreq);
                    }
                    LOGGER.fine("Added new freq amount to list.");
                }

                LOGGER.fine("Updating frequency in item " + item.getKey());
                // update frequency in item
                item.setParentFreq(nextFreq);
                // add item to nextFreq
                nextFreq.getEntries().add(item);
                // remove item from currentFreq
                if (currentFreq != null)
                    remove(currentFreq, item);

                log = "LFU after increment: ";
                for (FrequencyItem i : freqs)
                    log += i.toString() + " ";
                LOGGER.fine(log);
            }
        });
    }

    /**
     * Removes an item from the entries of a frequency level.
     * @param freq Frequency level to remove the item from.
     * @param item Item to remove
     */
    public synchronized void remove(FrequencyItem freq, CacheItem item) {
        LOGGER.info("Removing " + item.getKey() + " from freq list.");
        freq.getEntries().remove(item);
        if (freq.getEntries().isEmpty()) {
            freqs.remove(freq);
            LOGGER.fine("Frequency level " + freq.getFreq() + " empty, removing from list.");
        }
    }

    /**
     * Removes an item from the frequency list asynchronously.
     * @param item Item to remove.
     */
    public void removeAsync(CacheItem item) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                remove(item.getParentFreq(), item);
            }
        });
    }

    /**
     * Evicts {@code count} number of entries from the cache.
     * @param count The number of entries to remove.
     */
    public synchronized void evict(int count) {
        LOGGER.info("Evicting " + count + " entries from cache.");
        for (int i = 0; i < count;) {
            FrequencyItem f = freqs.peekFirst();
            if (f != null) {
                for (CacheItem item : f.getEntries())
                    if (i < count) {
                        LOGGER.fine("Evicting entry " + item.getKey() + " with frequency " + f.getFreq());
                        // remove entry from cache
                        cache.remove(item.getKey());
                        // remove entry from frequency level
                        remove(f, item);
                        currentSize--;
                        i++;
                    }
            } else
                // cache empty
                return;
        }
    }

    /**
     * Class holding a key value pair.
     */
    private class CacheItem {
        private String key;
        private String value;
        private FrequencyItem parentFreq;
        private ReadWriteLock lock;

        public CacheItem() {
            lock = new ReentrantReadWriteLock();
        }

        public CacheItem(String key, String value) {
            this.key = key;
            this.value = value;
            lock = new ReentrantReadWriteLock();
        }

        public ReadWriteLock getLock() {
            return lock;
        }

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
            return String.format("<%s, %s>", key, value);
        }
    }

    /**
     * Class holding a list of items for a specific frequency level.
     */
    private class FrequencyItem {
        // using list instead of hashmap, iterating over a couple entries is less performant
        // but allows to implement a fifo list for eviction
        private List<CacheItem> entries;
        private int freq;

        public FrequencyItem() {}

        public FrequencyItem(int freq) {
            entries = new LinkedList<>();
            this.freq = freq;
        }

        public List<CacheItem> getEntries() {
            return entries;
        }

        public void setEntries(List<CacheItem> entries) {
            this.entries = entries;
        }

        public int getFreq() {
            return freq;
        }

        public void setFreq(int freq) {
            this.freq = freq;
        }

        @Override
        public String toString() {
            String s = "(%s, [%s])";
            String s2 = "";

            Iterator<CacheItem> iter = entries.iterator();
            while(iter.hasNext())
                s2 += iter.next() + ", ";

            s2 = s2.substring(0, s2.length() - 2);
            return String.format(s, freq, s2);
        }
    }

    /**
     * Class executing runnables in series.
     */
    private class SerialExecutor implements Executor {
        // from https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Executor.html
        final Queue<Runnable> tasks = new ArrayDeque<Runnable>();
        final Executor executor;
        Runnable active;

        SerialExecutor(Executor executor) {
            this.executor = executor;
        }

        /**
         * Executes the given command at some time in the future.  The command
         * may execute in a new thread, in a pooled thread, or in the calling
         * thread, at the discretion of the {@code Executor} implementation.
         *
         * @param r the runnable task
         */
        @Override
        public synchronized void execute(final Runnable r) {
            tasks.offer(new Runnable() {
                public void run() {
                    try {
                        r.run();
                    } finally {
                        scheduleNext();
                    }
                }
            });
            if (active == null) {
                scheduleNext();
            }
        }

        protected synchronized void scheduleNext() {
            if ((active = tasks.poll()) != null) {
                executor.execute(active);
            }
        }
    }
}
