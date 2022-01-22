package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public interface TestStoreInterface {
    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param msg KVMessage containing key and value to put into the store.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    public KVMessage put(KVMessage msg) throws Exception;

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param msg KVMessage containing the key to get from the store.
     * @return the value, which is indexed by the given key.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    //public KVMessage get(KVMessage msg) throws Exception;

    public KVMessage get(KVMessage msg, String... password) throws IOException, IllegalStateException, SizeLimitExceededException, NoSuchAlgorithmException;

    /**
     * Deletes the value for a given key from the KVServer.
     *
     * @param msg KVMessage containing the key to delete from the store.
     * @return the last stored value of that key
     * @throws Exception if delete command cannot be executed (e.g. not connected to any KV server).
     */
    public KVMessage delete(KVMessage msg) throws Exception;

    public KVMessage unknownCommand(KVMessage msg) throws Exception;
}
