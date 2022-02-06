package de.tum.i13.Performance;

import de.tum.i13.client.SocketCommunicator;
import de.tum.i13.shared.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Enrondataset {

    private final static Logger LOGGER = Logger.getLogger(Enrondataset.class.getName());

    private static final int MAX_VAL_LENGTH = 122880;
    private static final String ENRON_DATASET = "C:/workspace/enron_mail_20150507/maildir/";

    private Path datasetPath;
    private TreeMap<String, String> dataLoaded;
    private List<Path> files = new ArrayList<>();

    public Enrondataset() throws IOException {
        this.datasetPath = Paths.get(ENRON_DATASET);
        if (!Files.exists(this.datasetPath))
            throw new IOException("Enron dataset was not found");
        loadFileLocations();
    }

    private void loadFileLocations() throws IOException {
        Files.walk(this.datasetPath)
                .filter(Files::isRegularFile)
                .forEach(filePath -> files.add(filePath));
        LOGGER.info(String.format("Number of entries in the dataset: %d", files.size()));
    }


    public void loadData(int amount) {
        this.dataLoaded = new TreeMap<>();
        Collections.shuffle(files);

        for (int i = 0; i < amount; i++) {
            Path filePath = this.files.get(i);
            readFile(filePath);
        }

        LOGGER.info("Numbers of data loaded " + dataLoaded.size());
    }

    private void readFile(Path filePath) {
        List<String> lines = null;
        try {
            lines = Files.readAllLines(filePath);
        } catch (IOException e) {
            LOGGER.info(String.format("Couldn't read file %s", filePath));
            return;
        }

        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = lines.iterator();
        String key = iter.next();
        while (iter.hasNext() && sb.length() <= MAX_VAL_LENGTH)
            sb.append(iter.next());

        if (sb.length() > MAX_VAL_LENGTH) {
            System.out.println("VALUE too large!");
            return;
        }
        String value = sb.toString();
        if(key.isEmpty() || key == null || value.isEmpty() || value == null) {
            LOGGER.info("Key or value is empty. Skipping file " + filePath.toString());
            return;
        }

        this.dataLoaded.put(key, value);
    }

    public void loadEntireDataset() {
        this.loadData(this.files.size());
    }

    public TreeMap<String, String> getDataLoaded() {
        return this.dataLoaded;
    }

    public int loadedDataSize() {
        return dataLoaded.size();
    }


}
