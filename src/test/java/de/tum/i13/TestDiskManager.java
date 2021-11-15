package de.tum.i13;

import de.tum.i13.client.ClientMessage;
import de.tum.i13.server.disk.DiskManager;
import de.tum.i13.server.kv.KVMessage;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDiskManager {

    DiskManager dm = DiskManager.getInstance();
    String key = "someKey";
    String value = "someValue";

    @Before
    public void initializeAll(){
        dm.initDiskManager("data");
    }


    @Test
    public void test1writeContent() {
        KVMessage result = dm.writeContent(new ClientMessage(KVMessage.StatusType.PUT, key, value));
        assertThat(result.getStatus(), is(KVMessage.StatusType.PUT_SUCCESS));
        File file = new File(dm.getW_path()+ key+".dat");
        assertThat(file.exists() && file.isFile(), equalTo(Boolean.TRUE));
    }

    @Test
    public void test2readContent() {

        KVMessage result = dm.readContent(new ClientMessage(KVMessage.StatusType.GET, key, null));
        assertThat(result.getStatus(), is(KVMessage.StatusType.GET_SUCCESS));
        assertThat(result.getValue(), equalTo(value));
    }

    @Test
    public void test3updateContent() {

        value = "thisisnewmenow";
        KVMessage result = dm.writeContent(new ClientMessage(KVMessage.StatusType.PUT, key, value));
        assertThat(result.getStatus(), is(KVMessage.StatusType.PUT_UPDATE));
        File file = new File(dm.getW_path()+ key+".dat");
        assertThat(result.getValue(), equalTo(value));
    }

    @Test
    public void test4deleteContent() {

        KVMessage result = dm.deleteContent(new ClientMessage(KVMessage.StatusType.DELETE, key, value));
        assertThat(result.getStatus(), is(KVMessage.StatusType.DELETE_SUCCESS));
        File file = new File(dm.getW_path()+ key+".dat");
        assertThat(!file.exists() && !file.isFile(), equalTo(Boolean.TRUE));
    }
}
