package de.tum.i13;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;


@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class KVIntegrationTest {

    public static Integer port = 5153;


    public String getResponse(Socket s)throws IOException{

        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        return input.readLine();

    }
    public String doRequest(Socket s, String req) throws IOException {
        OutputStream output = s.getOutputStream();
        //BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));

        //data = String.format("")
        output.write((req + "\r\n").getBytes(TELNET_ENCODING));
        output.flush();

        //String res = input.readLine();
        String res = getResponse(s);
        return res;
    }

    public String doRequest(String req) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port));
        String res = getResponse(s);
        res = doRequest(s, req);
        s.close();

        return res;
    }

    @Test
    public void test1_smokeTest() throws InterruptedException, IOException {
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
        th.interrupt();

        String command = "PUT newkey202 newvalue202";
        String response = doRequest(command);
        assertThat(response, is(equalTo("put_success newkey202 newvalue202")));

    }


    @Test
    public void test2_getNonExistentTest() throws InterruptedException, IOException {
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

        String command = "GET newkey213";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("get_error newkey213"));

    }

    @Test
    public void test3_getSuccessfulTest() throws InterruptedException, IOException {
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

        String command = "GET newkey202";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("get_success newkey202"));

    }

    @Test
    public void test4_deleteSuccessfulTest() throws InterruptedException, IOException {
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

        String command = "DELETE newkey202";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("delete_success newkey202"));

    }

    @Test
    public void test4_deleteErrorTest() throws InterruptedException, IOException {
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

        String command = "DELETE newkey202";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("delete_success newkey202"));

    }


    @Test
    public void getNonExistentTest() throws InterruptedException, IOException {
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

        String command = "GET newkey213";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("get_error newkey213"));

    }

    @Test
    public void enjoyTheEcho() throws IOException, InterruptedException {
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

        for (int tcnt = 0; tcnt < 10; tcnt++){
            final int finalTcnt = tcnt;
            new Thread(){
                @Override
                public void run() {
                    try {
                        Thread.sleep(finalTcnt * 100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        for(int i = 0; i < 100; i++) {

                           String command = "put hello " + finalTcnt;
                           String response = doRequest(command);

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

        Thread.sleep(5000);

        String response = doRequest("GET hello");
        assertEquals(response,"get_success hello 9");

        th.interrupt();


    }
}
