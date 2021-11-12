package de.tum.i13.server.cache;

import com.sun.security.ntlm.Server;
import de.tum.i13.server.disk.DiskManager;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

/**
 * Class implementing a FIFO cache to store kv-pairs.
 *
 * @version 0.1
 * @since   2021-11-06
 */
public class FIFO implements Cache{

    private final static Logger LOGGER = Logger.getLogger(FIFO.class.getName());

    // TODO: make this threadsafe
    private HashMap<String, String> cache;
    private LinkedList<String> fifo;
    private int maxSize;

    private static class Holder {
        private static final Cache INSTANCE = new FIFO();
    }

    private FIFO() {
        this.cache = null;
        this.fifo = null;
        this.maxSize = 0;
    }

    /**
     * Returns the cache instance.
     * @return  The cache instance.
     */
    public static Cache getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Initializes the cache data structure.
     *
     * @param maxSize the maximum number of keys to store in the cache.
     */
    @Override
    public void initCache(int maxSize) {
        // only init if cache is null
        if (this.cache == null) {
            LOGGER.info(String.format("Initialized FIFO cache with size %d", maxSize));
            cache = new HashMap<String, String>();
            fifo = new LinkedList<>();
            this.maxSize = maxSize;
        }
    }

    /**
     * Will put a kv-pair into the cache and optionally to the disk.
     *
     * @param msg KVMessage with key and value to be stored.
     * @return KVMessage with the result.
     */
    public KVMessage put(KVMessage msg) {
        // TODO: implement semaphore
        // if KVMessage does not have PUT command, return error
        if (msg.getStatus() != KVMessage.StatusType.PUT)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey());

        LOGGER.info(String.format("Put into cache: <%s, %s>", msg.getKey(), msg.getValue()));
        // insert into map
        if (cache.put(msg.getKey(), msg.getValue()) == null) {
            LOGGER.fine("Key was not in cache yet, adding...");
            // key not in fifo
            fifo.addFirst(msg.getKey());

            // check if fifo is full
            if (fifo.size() > maxSize) {
                LOGGER.info("Cache full, removing last...");
                // fifo is full --> remove last element from fifo and map
                cache.remove(fifo.removeLast());
            }
        }

        return new ServerMessage(KVMessage.StatusType.PUT_SUCCESS, msg.getKey(), msg.getValue());
    }

    /**
     * Gets the value for a key from the cache, reads from disk in case of cache miss.
     *
     * @param msg   KVMessage with key to get.
     * @return KVMessage with the result.
     */
    @Override
    public KVMessage get(KVMessage msg) {
        // if KVMessage does not have GET command, return error
        if (msg.getStatus() != KVMessage.StatusType.GET)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey());

        LOGGER.info(String.format("Getting cache value for %s", msg.getKey()));
        String value = cache.get(msg.getKey());
        if (value == null) {
            LOGGER.info("Key not in cache");
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), msg.getValue());
        }

        return new ServerMessage(KVMessage.StatusType.GET_SUCCESS, msg.getKey(), value);
    }

    /**
     * Deletes a kv-pair from the cache and the disk.
     *
     * @param msg   KVMessage with key to delete.
     * @return KVMessage with the result
     */
    @Override
    public KVMessage delete(KVMessage msg) {
        // if KVMessage does not have DELETE command, return error
        if (msg.getStatus() != KVMessage.StatusType.DELETE)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey());

        LOGGER.info(String.format("Deleting key from cache: %s", msg.getKey()));
        String value = cache.remove(msg.getKey());
        fifo.remove(msg.getKey());

        if (value != null)
            return new ServerMessage(KVMessage.StatusType.DELETE_SUCCESS, msg.getKey(), value);
        return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey());
    }
}
