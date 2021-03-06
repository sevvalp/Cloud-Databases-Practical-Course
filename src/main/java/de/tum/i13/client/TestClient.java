package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;

import javax.naming.SizeLimitExceededException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.Timer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;


import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Test Client
 * This class is intended as a CLI for a user to connect to a server and send messages.
 *
 * @version 0.2
 * @since   2021-10-25
 */
public class TestClient {
    private final static Logger LOGGER = Logger.getLogger(TestClient.class.getName());
    private final static Logger ROOT_LOGGER = Logger.getLogger("");
    private static TestStore store = new TestStore();
    static BufferedReader FromServer = null;

    private static boolean interpretInput(String input) throws SizeLimitExceededException, IOException, NoSuchAlgorithmException {

        if (input.length() == 0)
            return false;
        String[] command = input.split("\\s+");
        switch (command[0]) {
            case "connect":
                connect(command);
                FromServer = new BufferedReader(new InputStreamReader(store.getClientSocket().getInputStream()));
                break;
            case "disconnect":
                disconnect();
                break;
            case "send":
                send(command);
                break;
            case "logLevel":
                handleLogLevel(command);
                break;
            case "put":
                put(command);
                break;
            case "handleWithPassword":
                handleWithPassword(command);
                break;
            case "get":
                get(command);
                break;
            case "delete":
                delete(command);
                break;
            case "keyrange":
                keyRange();
                break;
            case "keyrange_read":
                keyRangeRead();
                break;
            case "receive":
                receive(command);
                break;
            case "subscribe":
                subscribe(command);
                break;
            case "unsubscribe":
                unsubscribe(command);
                break;
            case "quit":
                return true;
            default:
                printHelp();
                break;
        }
        return false;
    }


    /**
     * This method is used to handle the put request with password
     *
     * @param command array that contains the user's request
     */
    private static void handleWithPassword(String[] command) throws SizeLimitExceededException, IOException {
        if (command.length >= 1) {
            String msg = store.setInput(command);
            String message = "handleWithPassword ";
            store.activatePassword(message);
            System.out.println(msg);
        } else
            System.out.println("Unknown command ");
    }


    /**
     * Function used for debugging purposes. Will send any command, key, value to the connected server.
     * @param command The user input split into a String array.
     */
    private static void send(String[] command) {
        LOGGER.info("User is sending debug data: " + String.join(" ", command));
        if (command.length < 4)
            printHelp("send");
        else {
            try {
                StringJoiner v = new StringJoiner(" ");
                for (int i = 3; i < command.length; i++) {
                    v.add(command[i]);
                }
                String value = v.toString();
                System.out.println(store.send(command[1], command[2], value));
            } catch (IllegalStateException e) {
                System.out.println("Not connected to KVServer!");
            } catch (SizeLimitExceededException e) {
                System.out.println("The message to the server is too long!");
            } catch (IOException e) {
                System.out.println("IO error while sending.");
                LOGGER.severe(String.format("IO error: %s", e.getMessage()));
            }
        }
    }

    public static void receive(String[] command) {
        try {
            System.out.println(store.receive());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tries to connect to a server by calling {@link SocketCommunicator#connect(String, int)}.
     * @param command   The user input split into a String array.
     */
    private static void connect(String[] command) {
        LOGGER.info(String.format("User wants to connect to server with arguments: %s", String.join(" ", command)));
        if (command.length != 3)
            printHelp("connect");
        else {
            try {
                int port = Integer.parseInt(command[2]);
                LOGGER.info("Successfully connected to server.");
                System.out.println(store.connect(command[1], port));

            } catch (NumberFormatException e) {
                System.out.println("The port must be a number!");
            } catch (UnknownHostException e) {
                System.out.println("The Hostname or IP address of the host cannot be found!");
            } catch (IllegalStateException e) {
                System.out.println("Already connected to a KVServer!");
            } catch (IOException e) {
                System.out.printf("There was an error connecting to the server: %s%n", e.getMessage());
            }
        }
    }

    /**
     * Disconnects the socket by calling {@link SocketCommunicator#disconnect()}.
     */
    private static void disconnect() {
        try {
            System.out.println(store.disconnect());
        } catch (IllegalStateException e) {
            System.out.println("Not connected to a KVServer!");
        } catch (IOException e) {
            System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
        }
    }

    /**
     * Handles the extraction of the loglevel to be set for the Logger.
     *
     * @param command: The string input of the user
     */
    private static void handleLogLevel(String[] command) {
        LOGGER.info(String.format("User wants to set loglevel with arguments: %s", String.join(" ", command)));
            try {
                Level l = Level.parse(command[1]);
                ROOT_LOGGER.setLevel(l);
                for (Handler h : ROOT_LOGGER.getHandlers())
                    h.setLevel(l);

            } catch (Exception e) {
                printHelp("logLevel");
            }
            System.out.printf("Successfully set new Log Level %s%n", command[1]);
    }

    /**
     * Sends a put command to the server.
     * @param command   The user input split into a String array.
     */
    private static void put(String[] command) throws SizeLimitExceededException, IOException {
        LOGGER.info(String.format("User wants to put a key with arguments: %s", String.join(" ", command)));
        if (command.length < 3)
            printHelp("put");
        else {
            try {
                KVMessage msg = null;
                if(store.inputPassword.isInputPassword()) {

                    if (command.length != 4){
                        LOGGER.info("Password is not specified.");
                        System.out.println("Password is not specified.");
                        printHelp("put");
                        return;
                    } else{
                        if (store.inputPassword.getCountPasswordInput() < 4) {

                            StringJoiner v = new StringJoiner(" ");
                            for (int i = 2; i < command.length-1; i++) {
                                v.add(command[i]);
                            }
                            String value = v.toString();
                            String password = command[command.length - 1];

                            msg = store.put(new ClientMessage(KVMessage.StatusType.PUT, command[1], value, password));
                        }
                        else {
                            store.clearInput();
                            LOGGER.info("Exiting program");
                            System.out.println("Too many wrong password attempts, exiting program...");
                            try {
                                store.disconnect();
                            } catch (IOException e) {
                                System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
                            }
                            System.exit(0);

                        }
                    }
                }else {
                    StringJoiner v = new StringJoiner(" ");
                    for (int i = 2; i < command.length; i++) {
                        v.add(command[i]);
                    }
                    String value = v.toString();
                    msg = store.put(new ClientMessage(KVMessage.StatusType.PUT, command[1], value));

                }
                switch (msg.getStatus()) {
                    case PUT_SUCCESS: System.out.printf("Successfully put %s%n", msg.getKey()); break;
                    case PUT_UPDATE: System.out.printf("Successfully updated <%s, %s>%n", msg.getKey(), msg.getValue()); break;
                    case PUT_ERROR: System.out.printf("There was an error putting the value: %s%n", msg.getValue()); break;
                    case SERVER_WRITE_LOCK: System.out.printf("Storage server is currently blocked for write requests%n");break;
                    case SERVER_NOT_RESPONSIBLE:
                        //keyRange();
                        //added for performance testing purposes
                        if(store.getCorrectServer(command[1]) == "SUCCESS"){
                            put(command);
                        }
                        break;
                    case PASSWORD_WRONG: System.out.printf("Password wrong!%n");break;
                }
            } catch (IllegalStateException e) {
                System.out.println("Not connected to KVServer!");
            } catch (SizeLimitExceededException e) {
                System.out.println("The message to the server is too long!");
            } catch (IOException e) {
                System.out.println("IO error while sending.");
                LOGGER.severe(String.format("IO error: %s", e.getMessage()));
            } catch (Exception e) {
                System.out.println("Error while sending." + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Sends a get command to the server.
     * @param command   The user input split into a String array.
     */
    private static void get(String[] command) throws SizeLimitExceededException, IOException {
        LOGGER.info(String.format("User wants to get a key with arguments: %s", String.join(" ", command)));
        KVMessage msg = null;
        if(!store.inputPassword.isInputPassword()) {
            if (command.length != 2)
                printHelp("get");
            else
                msg = store.get(new ClientMessage(KVMessage.StatusType.GET, command[1]));
        }
        else {
            if (command.length != 3) {
                LOGGER.info("Password is not specified.");
                System.out.println("Password is not specified.");
                printHelp("get");
                return;
            }
            else if (store.inputPassword.getCountPasswordInput() < 4) {
                    msg = store.get(new ClientMessage(KVMessage.StatusType.GET, command[1],null, command[2]));
            }
            else {
                store.clearInput();
                LOGGER.info("Exiting program");
                System.out.println("Too many wrong password attempts, exiting program...");
                try {
                    store.disconnect();
                } catch (IOException e) {
                    System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
                }
                System.exit(0);

            }

            }
            try {
                //KVMessage msg = store.get(new ClientMessage(KVMessage.StatusType.GET, command[1], null));
                switch(msg.getStatus()) {
                    case GET_SUCCESS: System.out.printf("Get success: <%s, %s>%n", msg.getKey(), msg.getValue()); break;
                    case GET_ERROR: System.out.printf("There was an error getting the value: %s%n", msg.getValue());break;
                    case SERVER_NOT_RESPONSIBLE:
                        keyRange();
                        //added for performance testing purposes
                        if(store.getCorrectServer(command[1]) == "SUCCESS")
                            get(command);
                        break;
                    case PASSWORD_WRONG: System.out.printf("Password wrong!%n");break;
                }
            } catch (IllegalStateException e) {
                System.out.println("Not connected to KVServer!");
            } catch (SizeLimitExceededException e) {
                System.out.println("The message to the server is too long!");
            } catch (IOException e) {
                System.out.println("IO error while sending.");
                LOGGER.severe(String.format("IO error: %s", e.getMessage()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    /**
     * Sends a keyrange command to the server.
     */
    private static void keyRange() throws SizeLimitExceededException, IOException, NoSuchAlgorithmException {
        String message = "keyrange ";
        System.out.printf("keyrange: %s%n", store.sendKeyRange(message));


    }

    /**
     * Sends a keyrangeread command to the server.
     */
    private static void keyRangeRead() throws SizeLimitExceededException, IOException, NoSuchAlgorithmException {
        String message = "keyrange_read ";
        System.out.printf("keyrange_read: %s%n", store.sendKeyRange(message));
    }

    /**
     * Sends a delete command to the server.
     * @param command   The user input split into a String array.
     */
    private static void delete(String[] command) throws SizeLimitExceededException, IOException {
        LOGGER.info(String.format("User wants to delete a key with arguments: %s", String.join(" ", command)));
        KVMessage msg = null;
        if(!store.inputPassword.isInputPassword()) {
            if (command.length != 2)
                printHelp("delete");
            else
                msg = store.delete(new ClientMessage(KVMessage.StatusType.DELETE, command[1]));
        }
        else {
            if (command.length != 3){
                LOGGER.info("Password is not specified.");
                System.out.println("Password is not specified.");
                printHelp("delete");
                return;
            } else if (store.inputPassword.getCountPasswordInput() < 4) {
                msg = store.delete(new ClientMessage(KVMessage.StatusType.DELETE, command[1],null, command[2]));
            }
            else {
                store.clearInput();
                LOGGER.info("Exiting program");
                System.out.println("Too many wrong password attempts, exiting program...");
                try {
                    store.disconnect();
                } catch (IOException e) {
                    System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
                }
                System.exit(0);

            }
            try {
                //msg = store.delete(new ClientMessage(KVMessage.StatusType.DELETE, command[1]));
                switch (msg.getStatus()) {
                    case DELETE_SUCCESS: System.out.printf("Successfully deleted <%s, %s>%n", msg.getKey(), msg.getValue()); break;
                    case DELETE_ERROR: System.out.println("There was an error deleting the value.");break;
                    case SERVER_WRITE_LOCK: System.out.printf("Storage server is currently blocked for write requests%n");break;
                    case SERVER_NOT_RESPONSIBLE:
                        keyRange();
                        //added for performance testing purposes
                        if(store.getCorrectServer(command[1]) == "SUCCESS")
                            delete(command);
                        break;
                    case PASSWORD_WRONG: System.out.printf("Password wrong!%n");break;
                }
            } catch (IllegalStateException e) {
                System.out.println("Not connected to KVServer!");
            } catch (SizeLimitExceededException e) {
                System.out.println("The message to the server is too long!");
            } catch (IOException e) {
                System.out.println("IO error while sending.");
                LOGGER.severe(String.format("IO error: %s", e.getMessage()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Sends a subscribe command to the server.
     * @param command   The user input split into a String array.
     */
    private static void subscribe(String[] command) throws SizeLimitExceededException, IOException {
        LOGGER.info(String.format("User wants to subscribe a key with arguments: %s", String.join(" ", command)));
        if (command.length < 2)
            printHelp("subscribe");
        else {
            try {
                KVMessage msg = null;
                if(store.inputPassword.isInputPassword()) {

                    if (command.length < 3)
                        printHelp("subscribe");
                    else{
                        if (store.inputPassword.getCountPasswordInput() < 4) {

                            StringJoiner v = new StringJoiner(" ");
                            msg = store.subscribe(new ClientMessage(KVMessage.StatusType.SUBSCRBE, command[1], null, command[2]));
                        }
                        else {
                            store.clearInput();
                            LOGGER.info("Exiting program");
                            System.out.println("Too many wrong password attempts, exiting program...");
                            try {
                                store.disconnect();
                            } catch (IOException e) {
                                System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
                            }
                            System.exit(0);

                        }
                    }
                }else {
                    msg = store.subscribe(new ClientMessage(KVMessage.StatusType.SUBSCRBE, command[1], null));

                }
                switch (msg.getStatus()) {
                    case SUBSCRBE_OK: System.out.printf("Successfully subscribe to key %s %n", msg.getKey()); break;
                    case SERVER_WRITE_LOCK: System.out.printf("Storage server is currently blocked for write requests%n");break;
                    case SERVER_NOT_RESPONSIBLE:
                        //keyRange();
                        //added for performance testing purposes
                        if(store.getCorrectServer(command[1]) == "SUCCESS"){
                            subscribe(command);
                        }
                        break;
                    case PASSWORD_WRONG: System.out.printf("Password wrong!%n");break;
                }
            } catch (IllegalStateException e) {
                System.out.println("Not connected to KVServer!");
            } catch (SizeLimitExceededException e) {
                System.out.println("The message to the server is too long!");
            } catch (IOException e) {
                System.out.println("IO error while sending.");
                LOGGER.severe(String.format("IO error: %s", e.getMessage()));
            } catch (Exception e) {
                System.out.println("Error while sending." + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Sends a subscribe command to the server.
     * @param command   The user input split into a String array.
     */
    private static void unsubscribe(String[] command) throws SizeLimitExceededException, IOException {
        LOGGER.info(String.format("User wants to subscribe a key with arguments: %s", String.join(" ", command)));
        if (command.length < 2)
            printHelp("unsubscribe");
        else {
            try {
                KVMessage msg = null;
                if(store.inputPassword.isInputPassword()) {

                    if (command.length < 3)
                        printHelp("unsubscribe");
                    else{
                        if (store.inputPassword.getCountPasswordInput() < 4) {

                            StringJoiner v = new StringJoiner(" ");
                            msg = store.unsubscribe(new ClientMessage(KVMessage.StatusType.UNSUBSCRBE, command[1], null, command[2]));
                        }
                        else {
                            store.clearInput();
                            LOGGER.info("Exiting program");
                            System.out.println("Too many wrong password attempts, exiting program...");
                            try {
                                store.disconnect();
                            } catch (IOException e) {
                                System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
                            }
                            System.exit(0);

                        }
                    }
                }else {
                    msg = store.unsubscribe(new ClientMessage(KVMessage.StatusType.UNSUBSCRBE, command[1], null));

                }
                switch (msg.getStatus()) {
                    case UNSUBSCRBE_OK: System.out.printf("Successfully unsubscribed from key %s %n", msg.getKey()); break;
                    case UNSUBSCRBE_ERROR: System.out.printf("Error on unsubscribe from key %s %n", msg.getKey()); break;
                    case SERVER_WRITE_LOCK: System.out.printf("Storage server is currently blocked for write requests%n");break;
                    case SERVER_NOT_RESPONSIBLE:
                        //keyRange();
                        //added for performance testing purposes
                        if(store.getCorrectServer(command[1]) == "SUCCESS"){
                            subscribe(command);
                        }
                        break;
                    case PASSWORD_WRONG: System.out.printf("Password wrong!%n");break;
                }
            } catch (IllegalStateException e) {
                System.out.println("Not connected to KVServer!");
            } catch (SizeLimitExceededException e) {
                System.out.println("The message to the server is too long!");
            } catch (IOException e) {
                System.out.println("IO error while sending.");
                LOGGER.severe(String.format("IO error: %s", e.getMessage()));
            } catch (Exception e) {
                System.out.println("Error while sending." + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Prints a help page for the user.
     *
     * @param commands  List of commands to print the help for. Passing no args will print full help
     */
    private static void printHelp(String... commands) {
        LOGGER.info("Printing help");
        if (commands.length == 0) {
            System.out.println("This program allows to connect to a server through a socket. The server will echo the "
                    + "sent message, which will be printed on the CLI.");
            System.out.println("Commands:");
            System.out.println("connect <address> <port> - Tries to establish a TCP-connection to the echo "
                    + "server based on the given server address and the port number of the echo service.");
            System.out.println("disconnect - Tries to disconnect from the connected server.");
            System.out.println("send <command> <key> <message> - Sends a text message to the echo server according to the "
                    + "communication protocol.");
            System.out.println("put <key> <value> - Sends a key value pair to the server.");
            System.out.println("get <key> - Gets the value of the key from the server.");
            System.out.println("delete <key> - Deletes a value from the server.");
            System.out.println("logLevel <level> - Sets the logger to the specified log level.");
            System.out.println("help - Prints this help message.");
            System.out.println("quit - Disconnects from the server and exits the program execution.");
        }

        for (String c : commands) {
            switch (c) {
                case "connect": {
                    System.out.println("connect <address> <port> - Tries to establish a TCP-connection to the echo "
                            + "server based on the given server address and the port number of the echo service.");
                    System.out.println("\t<address> - Hostname or IP address of the echo server.");
                    System.out.println("\t<port> - The port of the echo service on the respective server.");
                    break;
                }
                case "disconnect": {
                    System.out.println("disconnect - Tries to disconnect from the connected server.");
                    break;
                }
                case "send": {
                    System.out.println("send <command> <key> <message> - Sends a text message to the echo server according to the "
                            + "communication protocol.");
                    System.out.println("\t<command> - command to send, e.g. PUT, GET");
                    System.out.println("\t<key> - Key to send");
                    System.out.println("\t<message> - Sequence of characters to be sent to the echo server.");
                    break;
                }
                case "put": {
                    System.out.println("put <key> <value> - Sends a key value pair to the server.");
                    System.out.println("\t<key> - The key under which the value should be stored.");
                    System.out.println("\t<value> - The value to be stored.");
                    break;
                }
                case "get": {
                    System.out.println("get <key> - Gets the value of the key from the server.");
                    System.out.println("\t<key> - The key to be searched for.");
                    break;
                }
                case "delete": {
                    System.out.println("delete <key> - Deletes a value from the server.");
                    System.out.println("\t<key> - The key to be deleted.");
                    break;
                }
                case "logLevel": {
                    System.out.println("logLevel <level> - Sets the logger to the specified log level.");
                    System.out.println("\t<level> - One of the following log levels: "
                           + "(ALL|SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST).");
                    break;
                }
                case "help": {
                    System.out.println("help - Prints this help message.");
                    break;
                }
                case "subscribe": {
                    System.out.println("subscribe <key> - Subscribe the key from the server.");
                    System.out.println("\t<key> - The key to be subscribed for.");
                    break;
                }
                case "unsubscribe": {
                    System.out.println("unsubscribe <key> - Unsubscribe the key from the server.");
                    System.out.println("\t<key> - The key to be unsubscribed from.");
                    break;
                }
                case "quit": {
                    System.out.println("quit - Disconnects from the server and exits the program execution.");
                    break;
                }
            }
        }
    }

    /**
     * Closes the connection of the socket and quits the program.
     */
    private static void quitProgram() {
        LOGGER.info("Exiting program");
        System.out.println("Exiting program...");
        try {
            store.disconnect();
        } catch (IOException e) {
            System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
        }
        System.exit(0);
    }


    /**
     * Main loop of the program. Asks the user for input indefinitely, until 'quit' is entered.
     *
     * @param args  Not used.
     */
    public static void main(String[] args) throws IOException {
        setupLogging(Paths.get("test.log"));
        Scanner scanner = new Scanner(System.in);

        if(store.getClientSocket().isConnected())
             FromServer = new BufferedReader(new InputStreamReader(store.getClientSocket().getInputStream()));
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));

        try {
            boolean quit = false;
            while (!quit) {

                if (FromServer!= null && FromServer.ready()) {
                    // receive from server
                    Subscriber sb = new Subscriber();
                    sb.handleRequests(FromServer.readLine());
                }

                if (inFromUser.ready()) {
                    System.out.print("EchoClient> ");
                    quit = interpretInput(scanner.nextLine());
                }


            }

            quitProgram();

        } catch (Exception e) {
            LOGGER.throwing(TestClient.class.getName(), "main", e);
        }
    }
}
