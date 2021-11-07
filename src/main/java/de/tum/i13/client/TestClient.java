package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.Scanner;
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
    private final static SocketCommunicator socketCommunicator = new SocketCommunicator();

    private static boolean interpretInput(String input) {
        if (input.length() == 0)
            return false;
        String[] command = input.split("\\s+");

        switch (command[0]) {
            case "connect":
                connect(command);
                break;
            case "disconnect":
                disconnect();
                break;
            /* not needed for MS2
            case "send":
                send(command);
                break;
             */
            case "logLevel":
                handleLogLevel(command);
                break;
            case "put":
                put(command);
                break;
            case "get":
                get(command);
                break;
            case "delete":
                delete(command);
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
                socketCommunicator.connect(command[1], port);
                LOGGER.info("Successfully connected to server.");
                System.out.println(socketCommunicator.receiveMessage());

            } catch (NumberFormatException e) {
                System.out.println("The port must be a number!");
            } catch (UnknownHostException e) {
                System.out.println("The Hostname or IP address of the host cannot be found!");
            } catch (IllegalStateException e) {
                System.out.println("Already connected to a KVServer!");
            } catch (IOException e) {
                System.out.printf("There was an error connecting to the server: %s", e.getMessage());
            }
        }
    }

    /**
     * Disconnects the socket by calling {@link SocketCommunicator#disconnect()}.
     */
    private static void disconnect() {
        try {
            socketCommunicator.disconnect();
        } catch (IllegalStateException e) {
            System.out.println("Not connected to a KVServer!");
        } catch (IOException e) {
            System.out.printf("There was an error while disconnecting from the server: %s", e.getMessage());
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
    private static void put(String[] command) {
        LOGGER.info(String.format("User wants to put a key with arguments: %s", String.join(" ", command)));
        if (command.length != 3)
            printHelp("put");
        else {
            try {
                KVMessage msg = socketCommunicator.put(command[1], command[2]);
                switch (msg.getStatus()) {
                    case PUT_SUCCESS: System.out.printf("Successfully put <%s, %s>", msg.getKey(), msg.getValue()); break;
                    case PUT_UPDATE: System.out.printf("Successfully updated <%s, %s>", msg.getKey(), msg.getValue()); break;
                    case PUT_ERROR: System.out.println("There was an error putting the value.");
                }
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

    /**
     * Sends a get command to the server.
     * @param command   The user input split into a String array.
     */
    private static void get(String[] command) {
        LOGGER.info(String.format("User wants to get a key with arguments: %s", String.join(" ", command)));
        if (command.length != 2)
            printHelp("get");
        else {
            try {
                KVMessage msg = socketCommunicator.get(command[1]);
                switch(msg.getStatus()) {
                    case GET_SUCCESS: System.out.printf("Get success: <%s, %s>", msg.getKey(), msg.getValue()); break;
                    case GET_ERROR: System.out.println("There was an error getting the value.");
                }
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

    /**
     * Sends a delete command to the server.
     * @param command   The user input split into a String array.
     */
    private static void delete(String[] command) {
        LOGGER.info(String.format("User wants to delete a key with arguments: %s", String.join(" ", command)));
        if (command.length != 2)
            printHelp("delete");
        else {
            try {
                KVMessage msg = socketCommunicator.delete(command[1]);
                switch (msg.getStatus()) {
                    case DELETE_SUCCESS: System.out.printf("Successfully deleted <%s, %s>", msg.getKey(), msg.getValue()); break;
                    case DELETE_ERROR: System.out.println("There was an error deleting the value.");
                }
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
            System.out.println("send <message> - Sends a text message to the echo server according to the "
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
                    System.out.println("send <message> - Sends a text message to the echo server according to the "
                            + "communication protocol.");
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
            socketCommunicator.disconnect();
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
    public static void main(String[] args) {
        setupLogging(Paths.get("test.log"));
        Scanner scanner = new Scanner(System.in);

        try {
            boolean quit = false;
            while (!quit) {
                System.out.print("EchoClient> ");
                quit = interpretInput(scanner.nextLine());
            }

            quitProgram();

        } catch (Exception e) {
            LOGGER.throwing(TestClient.class.getName(), "main", e);
        }
    }
}
