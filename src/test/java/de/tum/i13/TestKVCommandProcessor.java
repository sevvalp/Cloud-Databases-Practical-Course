package de.tum.i13;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.server.kv.KVStore;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

public class TestKVCommandProcessor {


    @Test
    public void correctParsingOfPut() throws Exception {

        KVStore kv = mock(KVServer.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process(null, "put key hello");

        ArgumentCaptor<KVMessage> argument = ArgumentCaptor.forClass(KVMessage.class);
        verify(kv).put(argument.capture());
        assertEquals("hello", argument.getValue().getValue());
        assertEquals("key", argument.getValue().getKey());
        assertEquals(KVMessage.StatusType.PUT, argument.getValue().getStatus());

    }

    @Test
    public void correctParsingOfGet() throws Exception {

        KVStore kv = mock(KVServer.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process(null, "get key");

        ArgumentCaptor<KVMessage> argument = ArgumentCaptor.forClass(KVMessage.class);
        verify(kv).get(argument.capture());
        assertEquals("key", argument.getValue().getKey());
        assertNull(argument.getValue().getValue());
        assertEquals(KVMessage.StatusType.GET, argument.getValue().getStatus());

    }

    @Test
    public void correctParsingOfDelete() throws Exception {

        KVStore kv = mock(KVServer.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process(null, "delete key");

        ArgumentCaptor<KVMessage> argument = ArgumentCaptor.forClass(KVMessage.class);
        verify(kv).delete(argument.capture());
        assertNull(argument.getValue().getValue());
        assertEquals("key", argument.getValue().getKey());
        assertEquals(KVMessage.StatusType.DELETE, argument.getValue().getStatus());

    }
    @Test
    public void correctParsingOfWrongCommand() throws Exception {

        KVStore kv = mock(KVServer.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process(null, "hello key");
      //  verify(kv).unknownCommand();

    }
}
