package de.tum.i13.client;

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
        String[] command = input.split("\\s+");

        switch (command[0]) {
            case "connect":
                connect(command);
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
            case "quit":
                return true;
            default:
                printHelp();
                break;
        }
        return false;
    }

    /**
     * Handles the extraction of the loglevel to be set for the Logger. The actual change of level happens in
     * {@link #setLogLevel(Level)}.
     *
     * @param command: The string input of the user
     */
    private static void handleLogLevel(String[] command) {
        LOGGER.info(String.format("User wants to set loglevel with arguments: %s", String.join(" ", command)));
        if (command.length != 2) {
            System.out.println("Cannot parse arguments! \nlogLevel <level>:\t\t\tSets the logger to the specified log "
                    + "level\n\t<level>:\t\t\t\tOne of the following log levels: "
                    + "(ALL|SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST)"
            );
        } else {
            switch(command[1]) {
                case "ALL": setLogLevel(Level.ALL); break;
                case "SEVERE": setLogLevel(Level.SEVERE); break;
                case "WARNING": setLogLevel(Level.WARNING); break;
                case "INFO": setLogLevel(Level.INFO); break;
                case "CONFIG": setLogLevel(Level.CONFIG); break;
                case "FINE": setLogLevel(Level.FINE); break;
                case "FINER": setLogLevel(Level.FINER); break;
                case "FINEST": setLogLevel(Level.FINEST); break;
                default: {
                    System.out.println("Cannot parse arguments! \nlogLevel <level>:\t\t\tSets the logger to the specified log "
                            + "level\n\t<level>:\t\t\t\tOne of the following log levels: "
                            + "(ALL|SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST)"
                    ); return;
                }
            }

            System.out.printf("Successfully set new Log Level %s%n", command[1]);
        }
    }

    /**
     * Sets a new logging level for the root logger as well as all handlers.
     *
     * @param level: The new level to be set.
     */
    private static void setLogLevel(Level level) {
        ROOT_LOGGER.setLevel(level);
        for (Handler h : ROOT_LOGGER.getHandlers()) {
            h.setLevel(level);
        }
        LOGGER.fine(String.format("Successfully set new level %s", level));
    }

    /**
     * Sends a message to the server and prints the response.
     *
     * @param command   The user input split into a String array.
     */
    private static void send(String[] command) {
        if (!socketCommunicator.isConnected()) {
            System.out.println("Not connected to server!");
            return;
        }

        String[] t = new String[command.length - 1];
        System.arraycopy(command, 1, t, 0, command.length - 1);
        String message = String.join(" ", t);

        LOGGER.info(String.format("User wants to send a message: %s", String.join(" ", command)));
        if (command.length > 1) {
            try {
                socketCommunicator.sendMessage(message);
                LOGGER.info("Successfully sent message");
                System.out.print(socketCommunicator.receiveMessage());
            } catch (IOException e) {
                System.out.printf("There was an error sending the message: %s%n", e.getMessage());
            }
        } else {
            System.out.println("There is no message to send!");
        }
    }

    /**
     * Tries to connect to a server by calling {@link SocketCommunicator#connect(String, int)}.
     * @param command   The user input split into a String array.
     */
    private static void connect(String[] command) {
        LOGGER.info(String.format("User wants to connect to server with arguments: %s", String.join(" ", command)));
        if (command.length != 3) {
            System.out.println("Cannot parse arguments! Usage:\nconnect <address> <port>:\tTries to establish a "
                    + "TCP-connection to the echo server based on the given server address and the port number of the "
                    + "echo service.\n\t<address>:\t\t\t\tHostname or IP address of the echo server.\n\t<port>:\t\t\t\t"
                    + "\tThe port of the echo service on the respective server."
            );
        } else {
            try {
                int port = Integer.parseInt(command[2]);
                if (socketCommunicator.connect(command[1], port)) {
                    LOGGER.info("Successfully connected to server");
                    System.out.print(socketCommunicator.receiveMessage());
                } else {
                    LOGGER.info("Already connected to server");
                    System.out.println("Already connected to server");
                }
            } catch (NumberFormatException e) {
                System.out.println("The port must be a number!");
            } catch (UnknownHostException e) {
                System.out.println("The Hostname or IP address of the host cannot be found!");
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
            socketCommunicator.disconnect();
        } catch (IOException e) {
            System.out.printf("There was an error while disconnecting from the server: %s%n", e.getMessage());
        }
    }

    /**
     * Prints a help page for the user.
     */
    private static void printHelp() {
        LOGGER.info("Printing help");
        System.out.println("This program allows to connect to a server through a socket. The server will echo the sent "
                + "message, which will be printed on the CLI.");
        System.out.println("Commands:");
        System.out.println("connect <address> <port> - Tries to establish a TCP-connection to the echo server based on "
                + "the given server address and the port number of the echo service.");
        System.out.println("\t<address> - Hostname or IP address of the echo server.");
        System.out.println("\t<port> - The port of the echo service on the respective server.");
        System.out.println("disconnect - Tries to disconnect from the connected server.");
        System.out.println("send <message> - Sends a text message to the echo server according to the communication "
                + "protocol.");
        System.out.println("\t<message> - Sequence of characters to be sent to the echo server.");
        System.out.println("logLevel <level> - Sets the logger to the specified log level.");
        System.out.println("\t<level> - One of the following log levels: "
                + "(ALL|SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST).");
        System.out.println("help - Prints this help message.");
        System.out.println("quit: - Disconnects from the server and exits the program execution.");
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
