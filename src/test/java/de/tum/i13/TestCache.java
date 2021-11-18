package de.tum.i13;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.server.cache.FIFO;
import de.tum.i13.server.cache.LFU;
import de.tum.i13.server.cache.LRU;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestCache {

    //TODO: test put, update, delete operations with multiple connected clients

    Cache cache;
    //Test simple put operation without exceeding the maximum limit
    private void testPut(int capacity, String cacheType){
        KVMessage result;

        if (cacheType.equals("LFU"))
            cache = LFU.getInstance();
        else if (cacheType.equals("LRU"))
            cache = LRU.getInstance();
        else
            cache = FIFO.getInstance();

        cache.initCache(capacity);

        for(int i=0; i<capacity; i++){
            result = cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+i, "value"+i));
            assertThat(result.getStatus(), is(KVMessage.StatusType.PUT_SUCCESS));
        }

    }

    private void testUpdate(int capacity, String cacheType){
        KVMessage result;

        if (cacheType.equals("LFU"))
            cache = LFU.getInstance();
        else if (cacheType.equals("LRU"))
            cache = LRU.getInstance();
        else
            cache = FIFO.getInstance();

        cache.initCache(capacity);

        for(int i=0; i<capacity; i++){
            result = cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+i, "value"+i));
        }

        for(int i=0; i<capacity; i++){
            String value  = "value" + String.valueOf(capacity-i);
            result = cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+i, value));
            //assertThat(result.getStatus(), is(KVMessage.StatusType.PUT_UPDATE));
            assertThat(result.getValue(), is(value));
        }

        for(int i=0; i<capacity; i++){
            String value  = "value" + String.valueOf(capacity-i);
            result = cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+i, null));
            assertThat(result.getStatus(), is(KVMessage.StatusType.GET_SUCCESS));
            assertThat(result.getValue(), is(value));
        }

    }

    private void testDelete(String cacheType){
        KVMessage result;

        if (cacheType.equals("LFU"))
            cache = LFU.getInstance();
        else if (cacheType.equals("LRU"))
            cache = LRU.getInstance();
        else
            cache = FIFO.getInstance();

        cache.initCache(1);
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+1, "value"+1));
        result = cache.delete(new ServerMessage(KVMessage.StatusType.DELETE, "key"+1, null));
        assertThat(result.getStatus(), is(KVMessage.StatusType.DELETE_SUCCESS));
        result = cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+1, null));
        assertThat(result.getStatus(), is(KVMessage.StatusType.GET_ERROR));
    }

    @Test
    public void testFIFOSimplePut(){
        testPut(100,"FIFO");
    }
    @Test
    public void testLFUSimplePut(){
        testPut(100,"LFU");
    }
    @Test
    public void testLRUSimplePut(){
        testPut(100,"LRU");
    }
    @Test
    public void testFIFOUpdate(){ testUpdate(10, "FIFO"); }
    @Test
    public void testLFUUpdate(){ testUpdate(10, "LFU"); }
    @Test
    public void testLRUpdate(){ testUpdate(10, "LRU");}
    @Test
    public void testFIFOPutStrategy(){
        // fullfill the capacity
        cache = FIFO.getInstance();
        cache.initCache(3);
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+1, "value"+1));
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+2, "value"+2));
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+3, "value"+3));

        // then try to add something else
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+4, "value"+4));
        KVMessage result = cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+1, null));

        // check the correct ordered thing deleted
        assertThat(result.getStatus(), is(KVMessage.StatusType.GET_ERROR));
        assertThat(B64Util.b64decode(result.getValue()), is("Key not in cache!"));

    }

    @Test
    public void testLRUPutStrategy(){
        //fullfill the capacity
        cache = LRU.getInstance();
        cache.initCache(3);
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+1, "value"+1));
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+2, "value"+2));
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+3, "value"+3));

        //play
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+2, null));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+3, null));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+1, null));

        // then try to add something else
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+4, "value"+4));
        KVMessage result = cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+2, null));

        // check the correct ordered thing deleted
        assertThat(result.getStatus(), is(KVMessage.StatusType.GET_ERROR));
        assertThat(B64Util.b64decode(result.getValue()), is("Key not in cache!"));

    }

    @Test
    public void testLFUPutStrategy(){

        //fullfill the capacity
        cache = LFU.getInstance();
        cache.initCache(3);
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+1, "value"+1));
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+2, "value"+2));
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+3, "value"+3));

        //play
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+2, "value"+3));
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+3, "value"+4));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+2, null));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+2, null));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+3, null));
        //cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+3, null));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+1, null));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+1, null));
        cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+1, null));

        // then try to add something else
        cache.put(new ServerMessage(KVMessage.StatusType.PUT, "key"+4, "value"+4));
        KVMessage result = cache.get(new ServerMessage(KVMessage.StatusType.GET, "key"+1, null));

        // check the correct ordered thing deleted
        assertThat(result.getStatus(), is(KVMessage.StatusType.GET_ERROR));
        assertThat(B64Util.b64decode(result.getValue()), is("Key not in cache!"));

    }

    @Test
    public void testFIFODelete(){ testDelete("FIFO"); }

    @Test
    public void testLRUDelete(){ testDelete("LRU"); }

    @Test
    public void testLFUDelete(){ testDelete("LFU"); }




}
