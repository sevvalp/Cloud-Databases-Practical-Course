package de.tum.i13.server.kv;


import java.nio.channels.SelectionKey;

public class ServerMessage implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;
    private final SelectionKey selectionKey;

    public ServerMessage (StatusType status, String key, String value) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.selectionKey = null;
    }

    public ServerMessage (StatusType status, String key, String value, SelectionKey selectionKey) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.selectionKey = selectionKey;
    }

    /**
     * @return the selection key that is associated with this message,
     * null if no selection key is associated.
     */
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    @Override
    public String getKey() {
        return key;
    }

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    @Override
    public String getValue() {
        return value;
    }

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    @Override
    public StatusType getStatus() {
        return status;
    }
}
