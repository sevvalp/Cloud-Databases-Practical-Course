package de.tum.i13.server.disk;

import de.tum.i13.server.kv.KVMessage;

import java.io.*;
import java.nio.charset.StandardCharsets;
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

    public String getW_path() {
        return w_path;
    }

    public void setW_path(String w_path) {
        this.w_path = System.getProperty("user.dir") + w_path + "\\";
    }

    public DiskManager() {
        this.w_path = System.getProperty("user.dir") + "\\data" + "\\";
    }

    /**
     * Write a kv-pair into the disk.
     *
     * @param key   Key to be stored.
     * @param value Value to be stored.
     */
    @Override
    public synchronized void writeContent(String key, String value) {

        try {
            String filepath = getW_path()+ key + ".dat";
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
                    fos.write(value.getBytes());
                } catch (IOException exception) {
                    LOGGER.severe("IO Exception while writing to file. " +  filepath);
                }
         //   }
        } catch (IOException e) {
            LOGGER.severe("IO Exception while writing to file."+  e.getMessage());
        }
    }

    /**
     * Read the value of a given key
     *
     * @param key   Key to be searched.
     */
    @Override
    public String readContent(String key) {

            ByteArrayOutputStream data = new ByteArrayOutputStream();
            String filepath = getW_path() + key + ".dat";

            File nFile = new File(filepath);

            if (nFile.exists()) {
                try {
                    byte[] value = Files.readAllBytes(Paths.get(filepath));
                    return new String(value,TELNET_ENCODING);
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
     * @param key   Key value to be deleted.
     */
    @Override
    public void deleteContent(String key) {
        LOGGER.info("Deleting key from disk.");

        String filepath = getW_path()+ key + ".dat";
        File dFile = new File(filepath);
        if(dFile.exists()){
            dFile.delete();
            LOGGER.info(key + " succesfully deleted from disk.");
        }else{
            LOGGER.info(key + " file could not be found.");
        }

    }

}
