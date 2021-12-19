package de.tum.i13;

import de.tum.i13.client.SocketCommunicator;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.shared.B64Util;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.naming.SizeLimitExceededException;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;


@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class KVIntegrationTest {

    public static Integer port = 5155;

    @BeforeAll
    private static void startServer() throws InterruptedException{
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    de.tum.i13.server.ecs.StartECS.main(new String[]{"-p", "5153", "-a", "127.0.0.1"});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start();
        Thread.sleep(2000);

        Thread thServer = new Thread() {
            @Override
            public void run() {
                try {
                    de.tum.i13.server.nio.StartSimpleNioServer.main(new String[]{"-a", "127.0.0.1", "-p", "5155", "-b", "127.0.0.1:5153"});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        thServer.start(); // started the server
        Thread.sleep(2000);

    }



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

        String command = "PUT newkey202 newvalue202";
        String response = doRequest(command);
        assertThat(response, is(equalTo("put_success newkey202 newvalue202")));

    }

    @Test
    public void test1_multiWorldValuePutSuccess() throws InterruptedException, IOException {

        String command = "PUT elephant one two three";
        String response = doRequest(command);
        assertThat(response, is(equalTo("put_success elephant one two three")));

    }


    @Test
    public void test2_getNonExistentTest() throws InterruptedException, IOException {

        String command = "GET newkey213";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("get_error newkey213"));

    }

    @Test
    public void test3_getSuccessfulTest() throws InterruptedException, IOException {

        String command = "GET newkey202";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("get_success newkey202"));

    }

    @Test
    public void test4_deleteSuccessfulTest() throws InterruptedException, IOException {


        String command = "DELETE newkey202";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("delete_success newkey202"));

    }

    @Test
    public void test5_deleteErrorTest() throws InterruptedException, IOException {


        String command = "DELETE newkey202";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("delete_error newkey202"));

    }


    @Test
    public void getNonExistentTest() throws InterruptedException, IOException {


        String command = "GET newkey213";
        String response = doRequest(command);
        assertThat(response, CoreMatchers.containsString("get_error newkey213"));

    }

    @Test
    public void unknownCommandTest() throws InterruptedException, IOException, SizeLimitExceededException {


        String command =  "arbitrary@++-- string 123\r\n";
        SocketCommunicator communicator = new SocketCommunicator();
        communicator.connect("127.0.0.1", 5155);
        String msg = new String(communicator.receive(), TELNET_ENCODING);
        communicator.send(command.getBytes(StandardCharsets.UTF_8));
        msg = new String(communicator.receive(), TELNET_ENCODING);
        //String response = doRequest(command);
        msg = msg.substring(0, msg.length()-2);
        String a = "error " + B64Util.b64encode("unknown command");
        assertThat(msg, CoreMatchers.containsString(a));

    }




    //@Test
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
