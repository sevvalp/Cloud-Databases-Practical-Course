package de.tum.i13.server.broker;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVServerCommunicator;
import de.tum.i13.server.kv.KVServerInfo;
import de.tum.i13.server.kv.ServerMessage;
import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.server.stripe.StripedCallable;
import de.tum.i13.server.stripe.StripedExecutorService;
import de.tum.i13.shared.B64Util;
import de.tum.i13.shared.Util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;

public class Broker {
    private Logger LOGGER = Logger.getLogger(Broker.class.getName());

    private SimpleNioServer server;
    private TreeMap<String, ArrayList<SelectionKey>> subscriptionKeys;

    private ExecutorService pool;

    public Broker() {
        this.pool = new StripedExecutorService();
        this.subscriptionKeys = new TreeMap<>();
    }

    /**
     * Sets the server to use to send messages to the client.
     *
     * @param server SimpleNioServer to use.
     */
    public void setServer(SimpleNioServer server) {
        this.server = server;
    }

    public KVMessage subscribe(KVMessage msg) throws IOException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));

        LOGGER.info(String.format("Client wants to subscribe key: %s", msg.getKey()));

        LOGGER.fine("Submitting new put callable to pool for key " + msg.getKey());
        // queue subscribe command
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.fine(String.format("Putting key into subscribe hashmap: %s", msg.getKey()));

                ArrayList<SelectionKey> selectionList = subscriptionKeys.get(msg.getKey());
                if (selectionList == null) {
                    selectionList = new ArrayList<SelectionKey>();
                    subscriptionKeys.put(msg.getKey(), selectionList);
                }
                selectionList.add(((ServerMessage) msg).getSelectionKey());

                String message;
                LOGGER.fine(String.format("Successfully added key into subribe list"));

                message = KVMessage.StatusType.SUBSCRBE_OK.name().toLowerCase() + " " + msg.getKey() +" "+ msg.getKey() +"\r\n";

                // return answer to client
                LOGGER.info("Answer to client: " + message);
                server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));

                return null;
            }

            public Object getStripe() {
                return msg.getKey();
            }
        });

        return null;
    }


    public KVMessage unsubscribe(KVMessage msg) throws IOException {
        // if server is not set, return error
        if (server == null)
            return new ServerMessage(KVMessage.StatusType.PUT_ERROR, msg.getKey(), B64Util.b64encode("Server is not set!"));

        LOGGER.info(String.format("Client wants to unsubscribe key: %s", msg.getKey()));

        LOGGER.fine("Submitting new put callable to pool for key " + msg.getKey());
        // queue subscribe command
        pool.submit(new StripedCallable<Void>() {
            public Void call() throws Exception {
                LOGGER.fine(String.format("Removing key into subscribe hashmap: %s", msg.getKey()));

                String message;
                ArrayList<SelectionKey> selectionList = subscriptionKeys.get(msg.getKey());
                if (selectionList == null) {
                    message = KVMessage.StatusType.UNSUBSCRBE_ERROR.name().toLowerCase() + " " + msg.getKey() + " " + msg.getKey() + "\r\n";
                } else {
                    int i = 0;
                    for (SelectionKey sk : selectionList) {
                        if (sk == ((ServerMessage) msg).getSelectionKey()) {
                            selectionList.remove(i);
                            break;
                        }
                        i++;
                    }

                    message = KVMessage.StatusType.UNSUBSCRBE_OK.name().toLowerCase() + " " + msg.getKey() + " " + msg.getKey() + "\r\n";

                }

                // return answer to client
                LOGGER.info("Answer to client: " + message);
                server.send(((ServerMessage) msg).getSelectionKey(), message.getBytes(TELNET_ENCODING));

                return null;
            }

            public Object getStripe() {
                return msg.getKey();
            }
        });
        return null;
    }

    public void update(String key, String command) throws UnsupportedEncodingException {

        //send subscribed clients message
        ArrayList<SelectionKey> selectionList = subscriptionKeys.get(key);
        if (selectionList != null) {
            for(SelectionKey sk : selectionList){
                    server.send(sk, command.getBytes(TELNET_ENCODING));
            }
        }

    }

    public void delete(String key, String command) throws UnsupportedEncodingException {

        ArrayList<SelectionKey> selectionList = subscriptionKeys.get(key);
        if (selectionList != null) {
            for(SelectionKey sk : selectionList){
                server.send(sk, command.getBytes(TELNET_ENCODING));
            }
            subscriptionKeys.remove(key);
        }

    }

    }

