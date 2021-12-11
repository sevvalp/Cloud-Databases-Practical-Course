package de.tum.i13.server.ecs;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.StringJoiner;
import java.util.logging.Logger;

public class ECSCommandProcessor implements CommandProcessor {
    private static final Logger LOGGER = Logger.getLogger(ECSCommandProcessor.class.getName());

    private ECSServer ecs;

    public ECSCommandProcessor(ECSServer ecs) {
        this.ecs = ecs;
    }

    @Override
    public void process(SelectionKey selectionKey, String command) throws Exception {
        LOGGER.info("Received command: " + command.trim());
        String[] request = command.split("\\s");
        request[0] = request[0].toLowerCase();

        StringJoiner v = new StringJoiner(" ");
        for (int i = 2; i < request.length; i++) {
            v.add(request[i]);
        }

        // TODO: handle requests
        // TODO: request of KVServer to shutdown
        switch (request[0]) {
            default:
                // handle unknown commands
                ecs.unknownCommand(new ServerMessage(KVMessage.StatusType.ERROR, "unknown", "command", selectionKey));
                LOGGER.info("Unknown command: " + String.join(" "));
        }
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: " + remoteAddress.toString());

        // TODO: calculate position of new server in storage ring
        // TODO: send keyrange to server
        // TODO: update metadata of server
        // TODO: set write lock
        // TODO: invoke transfer of data to new server

        return "Accepted connection from " + remoteAddress + " to KVServer ( " + address.toString() + ")\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {

    }
}