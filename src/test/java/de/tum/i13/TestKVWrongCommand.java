package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.Constants;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestKVWrongCommand {
    private static KVCommandProcessor kvcp;
    private static KVStore kvs;
    private static Path path = Paths.get("data/");

    @BeforeAll
    static void before() {

        if (!Files.exists(path)) {
            File dir = new File(path.toString());
            dir.mkdir();
        }
        kvs = new KVServer("FIFO", 100);
        kvcp = new KVCommandProcessor(kvs);
    }

    @Test
    void testWrongCommand(){
        try {
            assertEquals("Error. Wrong command.", kvcp.process(null, "Hello\r\n"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
