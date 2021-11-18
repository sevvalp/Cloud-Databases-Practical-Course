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
    public String process(SelectionKey selectionKey, String command) throws Exception {
        // just for testing
        // TODO: delete this
        LOGGER.info("Received command: " + command.trim());
        String[] c = command.split("\\s");
        /* TODO:
            - add selectionKey parameter, needed to write back result to NioServer asynchronously
            - split command and check for GET, PUT, DELETE --> return error message if unrecognized message
            - call respective method
         */
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

        return "Error. Wrong command.";
    }

    /**
     * Gets the value specified by the key.
     *
     * @param command   parsed command from user.
     */
    private String get(String[] command, SelectionKey selectionKey) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args etc.
            - call kvStore.get (include selectionKey in KVMessage!)
         */
        KVMessage kvmsg = new ServerMessage(KVMessage.StatusType.GET, command[1], null, selectionKey);

        if(command.length == 2) { //Process Get Command
            StartSimpleNioServer.logger.info("processing GET: " + command[1]);
            KVMessage cmd = null;
            try {
                cmd = kvStore.get(kvmsg);
                if(command != null) {
                    StartSimpleNioServer.logger.fine("GET succes: " + command[1]+ " " + cmd);
                    return "get_success " + command[1] + " " + cmd;
                } else {
                    StartSimpleNioServer.logger.fine("GET error: " + command[1]);
                    return "get_error " + command[1] + " key not found.";
                }
            } catch (Exception e) {
                e.printStackTrace();
                return "get_error " + command[1];
            }
        } else {
            return "get_error wrong number of parameters.";
        }
    }

    /**
     * Puts the key-value pair into the store.
     *
     * @param command   parsed command from user.
     */
    private String put(String[] command, SelectionKey selectionKey) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args etc.
            - call kvStore.put (include selectionKey in KVMessage!)
         */
        KVMessage kvmsg = new ServerMessage(KVMessage.StatusType.PUT, command[1], command[2], selectionKey);
        if(command.length < 3){
            return "put_error wrong number of parameters";
        }
        StartSimpleNioServer.logger.info("processing PUT: " + command[1]);
        try {
            //
            KVMessage ret = kvStore.put(kvmsg);
            if (ret.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                // NEW DATA
                StartSimpleNioServer.logger.fine("PUT success: " + command[1] + " " + command[2]);
                return "put_success " + command[1];
            } else if (ret.getStatus() == KVMessage.StatusType.PUT_UPDATE) {
                /// UPDATE
                StartSimpleNioServer.logger.fine("PUT update: " + command[1] + " " + command[2]);
                return "put_update " + command[1];
            } else  {
                //ERROR
                StartSimpleNioServer.logger.fine("PUT error: " + command[1] + " " + command[2]);
                return "put_error " + command[1];
            }
        } catch (Exception e) {
            StartSimpleNioServer.logger.fine("PUT error: " + command[1] + " " + command[2]);
            return "put_error " + command[1] + " " + command[2] ;
        }
    }

    /**
     * Deletes value from the store.
     *
     * @param command   parsed command from user.
     */
    private String delete(String[] command, SelectionKey selectionKey) {
        /* TODO:
            - add selectionKey parameter
            - check for correct number of args etc.
            - call kvStore.delete (include selectionKey in KVMessage!)
         */
        KVMessage kvmsg = new ServerMessage(KVMessage.StatusType.DELETE, command[1], null, selectionKey);
        if(command.length == 2) {
            StartSimpleNioServer.logger.info("processing DELETE: " + command[1]);
            KVMessage ret = null;
            try {
                ret = kvStore.delete(kvmsg);
                if (ret.getStatus() == KVMessage.StatusType.DELETE_SUCCESS) {
                    StartSimpleNioServer.logger.fine("DELETE success: " + command[1]);
                    return "delete_success " + command[1];
                } else {
                    StartSimpleNioServer.logger.fine("DELETE error: " + command[1] + " not found.");
                    return "delete_error " + command[1];
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        StartSimpleNioServer.logger.fine("PUT error: " + command[1]);
        return "delete_error wrong number of parameters.";
    }


    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        LOGGER.info("new connection: " + remoteAddress.toString());

        return "Accepted connection from " + remoteAddress.toString() + " to KVServer ( " + address.toString() + ")\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        LOGGER.info("Connection closed: " + address.toString());
    }
}
