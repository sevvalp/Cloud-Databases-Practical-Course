package de.tum.i13.Performance;

import de.tum.i13.Performance.Enrondataset;
import de.tum.i13.client.TestClient;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.TreeMap;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;
import org.junit.jupiter.api.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class PerformanceTest {
    private Enrondataset enronDataset;
    int opNr = 200; //128103;


    private void init() throws IOException {
        enronDataset = new Enrondataset();
        enronDataset.loadData(opNr);

    }

    private void startECS() throws InterruptedException {

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
    }

    private void startServers(int number) throws InterruptedException, IOException {

        for (int i=0; i<number; i++){
            int port = 5155 + i;
            Thread thServer = new Thread() {
                @Override
                public void run() {
                    try {
                        de.tum.i13.server.nio.StartSimpleNioServer.main(new String[]{"-a", "127.0.0.1", "-p", String.valueOf(port), "-b", "127.0.0.1:5153", "-ll", "OFF"});
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };
            thServer.start(); // started the server
            Thread.sleep(2000);
        }

    }

    private Socket[] startClients(int number, int servernr) throws IOException {
        Socket[] sockets = new Socket[number];

        for(int i= 0; i<number; i++){
            int port = 5155;
            if(servernr > 1){
                if(servernr >= number)
                    port = 5155 + i;
                else
                    port = 5156;
            }

            Socket s = new Socket();
            s.connect(new InetSocketAddress("127.0.0.1", port));
            getResponse(s);
            sockets[i] = s;
        }

        return sockets;
    }

    @Test
    public void oneServer_twoClient() throws InterruptedException, IOException {

        //init();
        enronDataset = new Enrondataset();
        enronDataset.loadData(200);
        TreeMap<String, String> loadadData1 = enronDataset.getDataLoaded();

//        enronDataset.loadData(100);
//        TreeMap<String, String> loadadData2 = enronDataset.getDataLoaded();

//        enronDataset.loadData(50);
//        TreeMap<String, String> loadadData3 = enronDataset.getDataLoaded();
//
//        enronDataset.loadData(50);
//        TreeMap<String, String> loadadData4 = enronDataset.getDataLoaded();

        startECS();

        startServers(4);

        Thread.sleep(60000);
        Socket[] sockets = startClients(1, 4);

        String req ="";

        // get timestamp here
        Timestamp beforeTS = new Timestamp(System.currentTimeMillis());
        long startTime = System.nanoTime();

        //TreeMap<String, String> loadadData = enronDataset.getDataLoaded();
        String res = "";
        opNr =  200; //loadadData.size();



        Thread thServer1 = new Thread() {
            @Override
            public void run() {
                try {
                    Socket s = sockets[0];
                    int x = 0;
                    for (String key: loadadData1.keySet()) {
                        String req = "put " + key + " " + loadadData1.get(key);
                        String res = doRequest(s, req);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

//        Thread thServer2 = new Thread() {
//            @Override
//            public void run() {
//                try {
//                    Socket s = sockets[1];
//                    int x = 0;
//                    for (String key: loadadData2.keySet()) {
//                        String req = "put " + key+ " " + loadadData2.get(key);
//                        String res = doRequest(s, req);
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        };

//        Thread thServer3 = new Thread() {
//            @Override
//            public void run() {
//                try {
//                    Socket s = sockets[2];
//                    int x = 0;
//                    for (String key: loadadData3.keySet()) {
//                        String req = "put " + key+ " " + loadadData3.get(key);
//                        String res = doRequest(s, req);
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//
//        Thread thServer4 = new Thread() {
//            @Override
//            public void run() {
//                try {
//                    Socket s = sockets[3];
//                    int x = 0;
//                    for (String key: loadadData4.keySet()) {
//                        String req = "put " + key+ " " + loadadData4.get(key);
//                        String res = doRequest(s, req);
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        };


        thServer1.start();
//        thServer2.start();
//        thServer3.start();
//        thServer4.start();

        thServer1.join();
//        thServer2.join();
//        thServer3.join();
//        thServer4.join();

        Thread.sleep(500);
//        Timestamp afterTS = new Timestamp(System.currentTimeMillis());
        long endTime   = System.nanoTime();

        writeResults("_one_client_four_server.txt", startTime, endTime);

        assertThat(true, equalTo(true));

    }

    @Test
    public void oneServer_oneClient() throws InterruptedException, IOException {

        init();


        //Start ECS
        startECS();

        //Start single server
        startServers(1);

        String req ="";
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", 5155));

        // get timestamp here
//        Timestamp beforeTS = new Timestamp(System.currentTimeMillis());
        long startTime = System.nanoTime();

        TreeMap<String, String> loadadData = enronDataset.getDataLoaded();
        String res = getResponse(s);
        opNr = loadadData.size();
        for (String key: loadadData.keySet()) {
            req = "put " + key + " " + loadadData.get(key);
            res = doRequest(s, req);
        }

//        Timestamp afterTS = new Timestamp(System.currentTimeMillis());
        long endTime   = System.nanoTime();

        writeResults("single_client_server.txt", startTime, endTime);

        s.close();

        assertThat(true, equalTo(true));

    }

    @Test
    public void testOneClientTwoServers() throws IOException, InterruptedException, NoSuchMethodException {

        init();

        //Start ECS
        startECS();

        //Start single server
        startServers(2);

        TestClient testApp = new TestClient();
        Class[] parameters = new Class[1];
        parameters[0] = java.lang.String.class;
        Method interpretInput = testApp.getClass().getDeclaredMethod("interpretInput", parameters);
        interpretInput.setAccessible(true);
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        try {
            interpretInput.invoke(testApp, "connect 127.0.0.1 5155");
            System.setOut(new PrintStream(outContent));

            TreeMap<String, String> loadadData = enronDataset.getDataLoaded();
            opNr = loadadData.size();

            long startTime = System.nanoTime();

            for (String key: loadadData.keySet()) {
                interpretInput.invoke(testApp, "put " + key + " " + loadadData.get(key) + "\r\n");
            }
            long endTime   = System.nanoTime();

            outContent.reset();

            writeResults("single_client_two_servers.txt", startTime, endTime);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

        assertThat(true, equalTo(true));

    }

    @Test
    public void testOneClientFourServers() throws IOException, InterruptedException, NoSuchMethodException {


        init();

        //Start ECS
        startECS();

        //Start single server
        startServers(4);

        TestClient testApp = new TestClient();
        Class[] parameters = new Class[1];
        parameters[0] = java.lang.String.class;
        Method interpretInput = testApp.getClass().getDeclaredMethod("interpretInput", parameters);
        interpretInput.setAccessible(true);
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        try {
            interpretInput.invoke(testApp, "connect 127.0.0.1 5155");
            System.setOut(new PrintStream(outContent));

            TreeMap<String, String> loadadData = enronDataset.getDataLoaded();
            opNr = loadadData.size();

            long startTime = System.nanoTime();

            for (String key: loadadData.keySet()) {
                interpretInput.invoke(testApp, "put " + key + " "+ loadadData.get(key) + "\r\n");
            }
            long endTime   = System.nanoTime();

            outContent.reset();

            writeResults("single_client_four_servers.txt", startTime, endTime);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

        assertThat(true, equalTo(true));

    }

    @Test
    public void testOneClientEightServers() throws IOException, InterruptedException, NoSuchMethodException {


        init();

        //Start ECS
        startECS();

        //Start single server
        startServers(8);

        TestClient testApp = new TestClient();
        Class[] parameters = new Class[1];
        parameters[0] = java.lang.String.class;
        Method interpretInput = testApp.getClass().getDeclaredMethod("interpretInput", parameters);
        interpretInput.setAccessible(true);
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        try {
            interpretInput.invoke(testApp, "connect 127.0.0.1 5155");
            System.setOut(new PrintStream(outContent));

            TreeMap<String, String> loadadData = enronDataset.getDataLoaded();
            opNr = loadadData.size();

            long startTime = System.nanoTime();

            for (String key: loadadData.keySet()) {
                interpretInput.invoke(testApp, "put " + key + " "+ loadadData.get(key) + "\r\n");
            }
            long endTime   = System.nanoTime();

            outContent.reset();

            writeResults("single_client_eight_servers.txt", startTime, endTime);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

        assertThat(true, equalTo(true));

    }


    public void writeResults(String fileName, long startTime, long endTime) throws IOException {

        long total_time = (long) ((endTime - startTime) / 1000000.0);
                /// 1000000.0);
        // for milliseconds: / 1000000.0);
        System.out.println("total time: " + total_time);

        Performance perf = new Performance();
        perf.withRuntime(total_time);
        perf.withNumOps(opNr);

        //write results to a file
        FileWriter fw = new FileWriter(fileName, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write("Total runtime (seconds): " + total_time);
        bw.newLine();
        bw.write("Total runtime (millisenconds): " + (long) ((endTime - startTime) /1000000.0));
        bw.newLine();
        bw.write("Number of operations " + perf.getNumOps());
        bw.newLine();
        bw.write("Throughput:  " + perf.getThroughput());
        bw.newLine();
        bw.write("Latency " + perf.getLatency());
        bw.close();

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



}
