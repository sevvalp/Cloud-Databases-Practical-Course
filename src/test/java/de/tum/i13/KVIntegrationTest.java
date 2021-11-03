package de.tum.i13;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class KVIntegrationTest {

    public static Integer port = 5153;

    public String doRequest(Socket s, String req) throws IOException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));

        output.write(req + "\r\n");
        output.flush();

        String res = input.readLine();
        return res;
    }

    public String doRequest(String req) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port));
        String res = doRequest(s, req);
        s.close();

        return res;
    }

    @Test
    public void smokeTest() throws InterruptedException, IOException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    de.tum.i13.server.nio.StartSimpleNioServer.main(new String[]{port.toString()});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);

        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port));
        String command = "hello ";
        assertThat(doRequest(command), is(equalTo(command)));
        s.close();

    }

    @Test
    public void enjoyTheEcho() throws IOException, InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    de.tum.i13.server.nio.StartSimpleNioServer.main(new String[]{port.toString()});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);

        for (int tcnt = 0; tcnt < 2; tcnt++){
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
                            Socket s = new Socket();
                            s.connect(new InetSocketAddress("127.0.0.1", port));
                            String command = "hello " + finalTcnt;
                            assertThat(doRequest(command), is(equalTo(command)));
                            s.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }



        //Assert.assertThat(doRequest("GET table key"), containsString("valuetest"));

        Thread.sleep(5000);
        th.interrupt();
    }
}
