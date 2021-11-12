package de.tum.i13.server.disk;

import de.tum.i13.server.kv.KVMessage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class DiskManager implements IDiskManager {

    private final static Logger LOGGER = Logger.getLogger(DiskManager.class.getName());
    private final ConcurrentMap<String, Object> writeLocks = new ConcurrentHashMap<>();
    private String w_path;

    private static class Holder {
        private static final DiskManager MANAGER = new DiskManager();
    }

    private DiskManager(){};

    /**
     * Returns the DiskManager instance.
     *
     * @return  The DiskManager instance.
     */
    public static DiskManager getInstance(){
        return Holder.MANAGER;
    }

    /**
     * Initializes the DiskManager
     * @param path  Path where the files shall be stored.
     */
    public void initDiskManager(String path) {
        this.w_path = path;
    }

    /**
     * Returns the path where the files will be stored.
     *
     * @return  Path where the files will be stored.
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
        // TODO: return KVMessage
        // TODO: check if manager is initialized (i.e. we have a path where to store the files)
        // TODO: return error if msg status is not PUT
        // TODO: Catch Exceptions here and return PUT_ERROR KVMessage
        // TODO: In case of error, supply message to be printed to client as value
        // TODO: Use B64Util.encode(<message>) to encode the message

        try {
            String filepath = getW_path()+ msg.getKey() + ".dat";
            LOGGER.info("Writing file path " + filepath);
            File nFile = new File(filepath);
            //  Object lock = writeLocks.get(filepath);

            //not sure if we need that
            //   synchronized (lock) {
                //if file exists delete old value
                if (nFile.exists())
                    new FileOutputStream(filepath).close();
                else {//else create a new file
                    if(!nFile.getParentFile().exists())
                        new File(getW_path()).mkdirs();
                    nFile.createNewFile();
                }

                try (FileOutputStream fos = new FileOutputStream(filepath)) {
                    fos.write(msg.getValue().getBytes());
                } catch (IOException exception) {
                    LOGGER.severe("IO Exception while writing to file. " +  filepath);
                }
         //   }
        } catch (IOException e) {
            LOGGER.severe("IO Exception while writing to file."+  e.getMessage());
        }

        return null;
    }

    /**
     * Read the value of a given key
     *
     * @param msg KVMessage with the key to read.
     * @return KVMessage with the result
     */
    @Override
    public KVMessage readContent(KVMessage msg) {
        // TODO: return KVMessage
        // TODO: check if manager is initialized (i.e. we have a path where to store the files)
        // TODO: return error if msg status is not GET
        // TODO: Catch Exceptions here and return GET_ERROR

            ByteArrayOutputStream data = new ByteArrayOutputStream();
            String filepath = getW_path() + msg.getKey() + ".dat";

            File nFile = new File(filepath);

            if (nFile.exists()) {
                try {
                    byte[] value = Files.readAllBytes(Paths.get(filepath));
                    // TODO: return KVMessage
                    //return new String(value,TELNET_ENCODING);
                    return null;
                } catch (IOException e) {
                    LOGGER.severe("Error during getting value "+ e.getMessage());
                }
            } else{
                LOGGER.severe("File not found on path: " + filepath);
            }

        return  null;
    }

    /**
     * Deletes a kv-pair from disk
     *
     * @param msg KVMessage with the key to delete.
     * @return KVMessage with the result.
     */
    @Override
    public KVMessage deleteContent(KVMessage msg) {
        // TODO: return KVMessage with the result
        // TODO: check if manager is initialized (i.e. we have a path where to store the files)
        // TODO: return error if msg status is not DELETE
        // TODO: return error if file could not be found
        LOGGER.info("Deleting key from disk.");

        String filepath = getW_path()+ msg.getKey() + ".dat";
        File dFile = new File(filepath);
        if(dFile.exists()){
            dFile.delete();
            LOGGER.info(msg.getKey() + " successfully deleted from disk.");
        }else{
            LOGGER.info(msg.getKey() + " file could not be found.");
        }

        return null;
    }

}
