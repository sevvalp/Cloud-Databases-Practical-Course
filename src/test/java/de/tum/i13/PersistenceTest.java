package de.tum.i13;

import de.tum.i13.client.SocketCommunicator;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.naming.SizeLimitExceededException;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PersistenceTest {
    @BeforeAll
    public static void beforeAll() throws IOException, InterruptedException, SizeLimitExceededException {

        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    de.tum.i13.server.nio.StartSimpleNioServer.main(new String[]{});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);
        String putResponse = doRequest("put hello454 world454");
        assertEquals("put_success hello454 world454", putResponse);
        String getResponse = doRequest("get hello454");
        assertEquals("get_success hello454 world454", getResponse);
        th.interrupt();

    }

    @Test
    public void persistenceTest() throws Exception {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    de.tum.i13.server.nio.StartSimpleNioServer.main(new String[]{});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);
        String getResponse = doRequest("get hello454");
        assertEquals("get_success hello454 world454", getResponse);

    }


    public static String getResponse(Socket s)throws IOException{

        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        return input.readLine();

    }
    public static String doRequest(Socket s, String req) throws IOException {
        OutputStream output = s.getOutputStream();
        output.write((req + "\r\n").getBytes(TELNET_ENCODING));
        output.flush();
        String res = getResponse(s);
        return res;
    }

    public static String doRequest(String req) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", 5153));
        String res = getResponse(s);
        res = doRequest(s, req);
        s.close();

        return res;
    }

}
