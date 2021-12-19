package de.tum.i13.server.kv;

import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.Util;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.StringJoiner;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class KVECSCommunicator implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(KVECSCommunicator.class.getName());

    private KVServer kvStore;
    private KVServerCommunicator kvServerECSCommunicator;

    private InetSocketAddress bootstrap;
    private String listenaddress;
    private int port;
    private int intraPort;

    //retrieve data from ECS
    private ServerSocket serverSocket;


    public KVECSCommunicator (KVStore kvStore, InetSocketAddress bootstrap, String listenaddress, int port, int intraPort) throws IOException {
        this.kvStore = (KVServer)kvStore;
        this.kvServerECSCommunicator = new KVServerCommunicator();
        this.bootstrap = bootstrap;
        this.listenaddress = listenaddress;
        this.port = port;
        this.intraPort = intraPort;

        this.serverSocket = new ServerSocket();
    }

    @Override
    public void run() {
        addShutDownHook();
        connectECS();
        process();
    }

    private void process(){
//        while(true) {
//            try {


//                //String msg = new String(kvServerECSCommunicator.receive(), TELNET_ENCODING);
//
//                Socket socket = serverSocket.accept();
//                ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
//                oos.flush();
//                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
//                String msg = new String(kvServerECSCommunicator.receive(ois), TELNET_ENCODING);
//
//
//                if(msg != null && !msg.isEmpty()) {
//                    LOGGER.info("Received command: " + msg.trim());
//                    String[] request = msg.substring(0, msg.length() - 2).split("\\s");
//                    int size = request.length;
//                    request[0] = request[0].toLowerCase();
//
//                    StringJoiner v = new StringJoiner(" ");
//                    for (int i = 2; i < request.length; i++) {
//                        v.add(request[i]);
//                    }
//
//                    switch (request[0]) {
//                        case "ecs_accept":
//                            LOGGER.info("ECS accepted connection.");
//                            break;
//                        case "ecs_error":
//                            LOGGER.info("Got error from ECS.");
//                            break;
//                        case "rebalance":
//                            kvStore.rebalance(new ServerMessage(KVMessage.StatusType.REBALANCE, request[1], request[2]));
//                            sendRebalanceSuccess(request[1], request[2]);
//                            break;
//                        case "update_metadata":
//                            kvStore.receiveMetadata(new ServerMessage(KVMessage.StatusType.UPDATE_METADATA, request[1], request[2]));
//                            break;
//                    }
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
       // }
    }


    private void sendRebalanceSuccess(String addressPort, String hash) throws Exception {
        String msg = "rebalance_success " + addressPort + " " + hash + "\r\n";
        kvServerECSCommunicator.send(msg.getBytes(TELNET_ENCODING));
    }


    public void connectECS(){
        LOGGER.info("Connecting to ECS");
        try {
            //connect to ECS

            kvServerECSCommunicator.connect(this.bootstrap.getAddress().getHostAddress(), this.bootstrap.getPort());
            kvServerECSCommunicator.receive();
            //notify ECS that new server added
            // NEWSERVER <encoded info: address,port,intraport>
            String command = "newserver ";
            String b64Value = B64Util.b64encode(String.format("%s,%s,%s", this.listenaddress, this.port, this.intraPort));
            String message = String.format("%s %s\r\n", command.toUpperCase(), b64Value);
            LOGGER.info("Message to server: " + message);

            LOGGER.info("Notify ECS that new server added");
            kvServerECSCommunicator.send(message.getBytes(TELNET_ENCODING));

//            String msg = new String(kvServerECSCommunicator.receive(), TELNET_ENCODING);
//            String[] request = msg.substring(0, msg.length() - 2).split("\\s");
//            kvStore.receiveMetadata(new ServerMessage(KVMessage.StatusType.UPDATE_METADATA, request[1], request[2]));

            kvServerECSCommunicator.disconnect();

        }catch (Exception e){
            LOGGER.info("Exception while connecting ECS ");
        }
    }

    private void addShutDownHook(){
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {

//                kvStore.gracefullyShutdown();
                //send ecs historic data
                LOGGER.info("Notify ECS gracefully shut down.");
                try {
                    String message = String.format("%s %s\r\n", "removeserver", B64Util.b64encode(String.format("%s,%s,%s", listenaddress, port, intraPort)));
                    LOGGER.info("Message to ECS: " + message);
                    kvServerECSCommunicator.send(message.getBytes(TELNET_ENCODING));
                    LOGGER.info("Notified ECS gracefully shut down.");
                    kvServerECSCommunicator.disconnect();
                } catch (Exception e) {
                    LOGGER.info("ECS Exception while stopping server.");
                }

            }
        });
    }

}
