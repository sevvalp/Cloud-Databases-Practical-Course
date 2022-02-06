package de.tum.i13.server.broker;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.StringJoiner;
import java.util.logging.Logger;

public class BrokerCommandProcessor implements CommandProcessor {
    private static final Logger LOGGER = Logger.getLogger(BrokerCommandProcessor.class.getName());

    private Broker broker;

    public BrokerCommandProcessor(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void process(SelectionKey selectionKey, String command) throws Exception {
        LOGGER.info("Received command: " + command.trim());
        String[] request = command.split("\\s");
        request[0] = request[0].toLowerCase();

        if (request != null) {
            switch (request[0]) {
                case ("subscribe_delete"):
                    System.out.println("Subscribed key deleted: "+ B64Util.b64decode(request[1]));
                    broker.delete(request[1], command);
                    break;
                case ("subscribe_update"):
                    broker.update(request[1], command);
                    break;
                case "subscribe":
                    broker.subscribe(new ServerMessage(KVMessage.StatusType.SUBSCRBE, request[1], null, selectionKey));
                    break;
                case "unsubscribe":
                    broker.unsubscribe(new ServerMessage(KVMessage.StatusType.UNSUBSCRBE, request[1], null, selectionKey));
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: " + remoteAddress.toString());

        return "ECS_ACCEPT\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {

    }
}
