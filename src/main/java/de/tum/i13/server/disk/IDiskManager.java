package de.tum.i13.server.disk;

import java.io.UnsupportedEncodingException;

public interface IDiskManager {

    /**
     * Write a kv-pair into the disk.
     *
     * @param key   Key to be stored.
     * @param value Value to be stored.
     */
    void writeContent(String key, String value) throws UnsupportedEncodingException;

    /**
     * Read the value of a given key
     *
     * @param key   Key to be searched.
     */
    String readContent(String key) throws UnsupportedEncodingException;

    /**
     * Deletes a kv-pair from disk
     *
     * @param key   Key value to be deleted.
     */
    void deleteContent(String key);
}
