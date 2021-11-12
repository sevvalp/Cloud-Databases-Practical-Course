package de.tum.i13.server.disk;

import de.tum.i13.server.kv.KVMessage;

import java.io.UnsupportedEncodingException;

public interface IDiskManager {

    /**
     * Write a kv-pair into the disk.
     *
     * @param msg KVMessage with the key and value to store.
     * @return KVMessage with the result.
     */
    KVMessage writeContent(KVMessage msg);

    /**
     * Read the value of a given key
     *
     * @param msg KVMessage with the key to read.
     * @return KVMessage with the result.
     */
    KVMessage readContent(KVMessage msg);

    /**
     * Deletes a kv-pair from disk
     *
     * @param msg KVMessage with the key to delete.
     * @return KVMessage with the result.
     */
    KVMessage deleteContent(KVMessage msg);
}
