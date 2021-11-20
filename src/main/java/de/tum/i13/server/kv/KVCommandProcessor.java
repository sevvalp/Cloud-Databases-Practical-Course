package de.tum.i13.server.kv;

import de.tum.i13.server.cache.Cache;
import de.tum.i13.server.nio.StartSimpleNioServer;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.logging.Logger;
import java.util.logging.Level;

public class KVCommandProcessor implements CommandProcessor {
    private static final Logger LOGGER = Logger.getLogger(KVCommandProcessor.class.getName());

    private KVStore kvStore;

    public KVCommandProcessor (KVStore kvStore) {
        this.kvStore = kvStore;
        StartSimpleNioServer.logger.setLevel(Level.ALL);
        // TODO: handle args
        // TODO: log stuff
    }

    @Override
    public void process(SelectionKey selectionKey, String command) throws Exception {
        LOGGER.info("Received command: " + command.trim());
        String[] c = command.split("\\s");
        String[] request = command.split(" ");
        int size = request.length;
        request[size-1] = request[size-1].replace("\r\n", "");
        request[0] = request[0].toLowerCase();

        try {
            if (request[0].equals("put"))
                kvStore.put(new ServerMessage(KVMessage.StatusType.PUT, request[1], request[2], selectionKey));
            /*else*/ if (request[0].equals("get"))
                kvStore.get(new ServerMessage(KVMessage.StatusType.GET, request[1], null, selectionKey));
            else if (request[0].equals("delete"))
                kvStore.delete(new ServerMessage(KVMessage.StatusType.DELETE, request[1], null, selectionKey));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets the value specified by the key.
     *
     * @param command   parsed command from user.
     */
    private String get(String[] command, SelectionKey selectionKey) {

        if(command.length == 2) { //Process Get Command
            KVMessage kvmsg = new ServerMessage(KVMessage.StatusType.GET, command[1], null, selectionKey);
            StartSimpleNioServer.logger.info("processing GET: " + command[1]);
            try {
                KVMessage cmd = kvStore.get(kvmsg);
                if(cmd.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
                    StartSimpleNioServer.logger.fine(KVMessage.StatusType.GET_SUCCESS + " " + command[1]+ " " + cmd);
                    return KVMessage.StatusType.GET_SUCCESS + command[1] + " " + cmd;
                } else {
                    StartSimpleNioServer.logger.fine(KVMessage.StatusType.GET_ERROR + " " + command[1]);
                    return KVMessage.StatusType.GET_ERROR + " " + command[1] + " key not found.";
                }
            } catch (Exception e) {
                e.printStackTrace();
                return KVMessage.StatusType.GET_ERROR + " " + command[1];
            }
        } else {
            return KVMessage.StatusType.GET_ERROR + " wrong number of parameters.";
        }
    }

    /**
     * Puts the key-value pair into the store.
     *
     * @param command   parsed command from user.
     */
    private String put(String[] command, SelectionKey selectionKey) {
        if(command.length < 3){
            return KVMessage.StatusType.PUT_ERROR + " " + "wrong number of parameters";
        }
        StartSimpleNioServer.logger.info("processing PUT: " + " " + command[1]);

        StringBuilder v = new StringBuilder();
        for (int i = 2; i < command.length; i++) {
            v.append(command[i]);
        }
        String value = v.toString();
        KVMessage kvmsg = new ServerMessage(KVMessage.StatusType.PUT, command[1], value, selectionKey);
        try {
            //
            KVMessage ret = kvStore.put(kvmsg);
            if (ret.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                // NEW DATA
                StartSimpleNioServer.logger.fine(KVMessage.StatusType.PUT_SUCCESS + " " + command[1] + " " + value);
                return KVMessage.StatusType.PUT_SUCCESS + " " + command[1];
            } else if (ret.getStatus() == KVMessage.StatusType.PUT_UPDATE) {
                /// UPDATE
                StartSimpleNioServer.logger.fine(KVMessage.StatusType.PUT_UPDATE + " " + command[1] + " " + value);
                return KVMessage.StatusType.PUT_UPDATE + " " + command[1];
            } else  {
                //ERROR
                StartSimpleNioServer.logger.fine(KVMessage.StatusType.PUT_ERROR + " " + command[1] + " " + value);
                return KVMessage.StatusType.PUT_ERROR + " " + command[1];
            }
        } catch (Exception e) {
            StartSimpleNioServer.logger.fine(KVMessage.StatusType.PUT_ERROR + " " + command[1] + " " + value);
            return KVMessage.StatusType.PUT_ERROR + command[1] + " " + value ;
        }
    }

    /**
     * Deletes value from the store.
     *
     * @param command   parsed command from user.
     */
    private String delete(String[] command, SelectionKey selectionKey) {
        if(command.length == 2) {
            KVMessage kvmsg = new ServerMessage(KVMessage.StatusType.DELETE, command[1], null, selectionKey);
            StartSimpleNioServer.logger.info(KVMessage.StatusType.DELETE + " " + command[1]);
            try {
                KVMessage ret = kvStore.delete(kvmsg);
                if (ret.getStatus() == KVMessage.StatusType.DELETE_SUCCESS) {
                    StartSimpleNioServer.logger.fine(KVMessage.StatusType.DELETE_SUCCESS + " " + command[1]);
                    return KVMessage.StatusType.DELETE_SUCCESS + " " + command[1];
                } else {
                    StartSimpleNioServer.logger.fine(KVMessage.StatusType.DELETE_ERROR + " " + command[1] + " not found.");
                    return KVMessage.StatusType.DELETE_ERROR + " " + command[1];
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        StartSimpleNioServer.logger.fine(KVMessage.StatusType.DELETE_ERROR + " " + command[1]);
        return KVMessage.StatusType.DELETE_ERROR + " wrong number of parameters.";
    }


    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: " + remoteAddress.toString());

        return "Accepted connection from " + remoteAddress + " to KVServer ( " + address.toString() + ")\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        LOGGER.info("Connection closed: " + address.toString());
    }
}
