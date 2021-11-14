package de.tum.i13.server.disk;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class DiskManager implements IDiskManager {

    private final static Logger LOGGER = Logger.getLogger(DiskManager.class.getName());
    private String w_path;

    private static class Holder {
        private static final DiskManager MANAGER = new DiskManager();
    }

    private DiskManager() {
    }

    /**
     * Returns the DiskManager instance.
     *
     * @return The DiskManager instance.
     */
    public static DiskManager getInstance() {
        return Holder.MANAGER;
    }

    /**
     * Initializes the DiskManager
     *
     * @param path Path where the files shall be stored.
     */
    public void initDiskManager(String path) {
        this.w_path = path;
        if (this.w_path.charAt(this.w_path.length() - 1) != '/')
            this.w_path += "/";
    }

    /**
     * Returns the path where the files will be stored.
     *
     * @return Path where the files will be stored.
     */
    public String getW_path() {
        return w_path;
    }

    /**
     * Write a kv-pair into the disk.
     *
     * @param msg KVMessage with the key and value to store.
     * @return KVMessage with the result
     */
    @Override
    public KVMessage writeContent(KVMessage msg) {

        if (this.w_path == null)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Disk Manager is not yet initialized!"));

        if (msg.getStatus() != KVMessage.StatusType.PUT)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Put into disk: <%s, %s>", msg.getKey(), msg.getValue()));
        KVMessage.StatusType status;
        try {
            String filepath = getW_path() + msg.getKey() + ".dat";
            File nFile = new File(filepath);

            //if file exists delete old value
            if (nFile.exists()) {
                new FileOutputStream(filepath).close();
                status = KVMessage.StatusType.PUT_UPDATE;
            } else {//else create a new file
                if (nFile.createNewFile())
                    status = KVMessage.StatusType.PUT_SUCCESS;
                else
                    return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Error occurred while writing to disk!"));
            }
            FileOutputStream fos = new FileOutputStream(filepath);
            fos.write(msg.getValue().getBytes());
            fos.close();

        } catch (IOException e) {
            LOGGER.info("IO Exception while writing to file.");
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Error occurred while writing to disk!"));
        }

        return new ServerMessage(status, msg.getKey(), msg.getValue());
    }

    /**
     * Read the value of a given key
     *
     * @param msg KVMessage with the key to read.
     * @return KVMessage with the result
     */
    @Override
    public KVMessage readContent(KVMessage msg) {

        if (this.w_path == null)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Disk Manager is not yet initialized!"));

        if (msg.getStatus() != KVMessage.StatusType.GET)
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Getting value from disk for %s", msg.getKey()));
        String filepath = getW_path() + msg.getKey() + ".dat";
        File nFile = new File(filepath);

        if (nFile.exists()) {
            try {
                byte[] value = Files.readAllBytes(Paths.get(filepath));
                return new ServerMessage(KVMessage.StatusType.GET_SUCCESS, msg.getKey(), new String(value, TELNET_ENCODING));
            } catch (IOException e) {
                LOGGER.info("IOException occurred while reading the from disk.");
                return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Exception occurred while getting key!"));
            }
        } else {
            LOGGER.info("Key not in disk");
            return new ServerMessage(KVMessage.StatusType.GET_ERROR, msg.getKey(), B64Util.b64encode("Key not in disk!"));
        }

    }

    /**
     * Deletes a kv-pair from disk
     *
     * @param msg KVMessage with the key to delete.
     * @return KVMessage with the result.
     */
    @Override
    public KVMessage deleteContent(KVMessage msg) {

        if (this.w_path == null)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Disk Manager is not yet initialized!"));

        if (msg.getStatus() != KVMessage.StatusType.DELETE)
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("KVMessage does not have correct status!"));

        LOGGER.info(String.format("Deleting key from disk: %s", msg.getKey()));

        String filepath = getW_path() + msg.getKey() + ".dat";
        File dFile = new File(filepath);
        try {
            if (dFile.exists()) {
                byte[] value = Files.readAllBytes(Paths.get(filepath));
                if (dFile.delete())
                    return new ServerMessage(KVMessage.StatusType.DELETE_SUCCESS, msg.getKey(), new String(value, TELNET_ENCODING));
                else
                    return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Exception occurred while deleting key!"));
            } else {
                LOGGER.info(" Key not in disk");
                return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Key not in disk!"));
            }
        } catch (IOException e) {
            LOGGER.info("IOException occurred while deleting key the from disk.");
            return new ServerMessage(KVMessage.StatusType.DELETE_ERROR, msg.getKey(), B64Util.b64encode("Exception occurred while deleting key!"));
        }

    }

}
