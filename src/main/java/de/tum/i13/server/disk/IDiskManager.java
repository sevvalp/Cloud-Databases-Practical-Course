package de.tum.i13.server.disk;

import de.tum.i13.server.kv.KVMessage;

import java.io.UnsupportedEncodingException;

public interface IDiskManager {

    /**
     * Write a kv-pair into the disk.
     *
     * @param key   Key to be stored.
     * @param value Value to be stored.
     * @return KVMessage with the result.
     */
    KVMessage writeContent(String key, String value);

    /**
     * Read the value of a given key
     *
     * @param key   Key to be searched.
     * @return KVMessage with the result.
     */
    KVMessage readContent(String key);

    /**
     * Deletes a kv-pair from disk
     *
     * @param key   Key value to be deleted.
     * @return KVMessage with the result.
     */
    KVMessage deleteContent(String key);
}
