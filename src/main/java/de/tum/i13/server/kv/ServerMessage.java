package de.tum.i13.server.kv;


import de.tum.i13.shared.Metadata;

import java.nio.channels.SelectionKey;

public class ServerMessage implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;
    private final SelectionKey selectionKey;
    private final Metadata metadata;
    private String password = null;

    public ServerMessage (StatusType status, String key, String value, String... password) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.selectionKey = null;
        this.metadata = null;
        if(password.length > 0)
            this.password = password[0];
    }

    public ServerMessage (StatusType status, String key, String value, SelectionKey selectionKey, String... password) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.selectionKey = selectionKey;
        this.metadata = null;
        if(password.length > 0)
            this.password = password[0];
    }

    public ServerMessage (StatusType status, Metadata metadata, String... password) {
        this.status = status;
        this.metadata = metadata;
        this.key = null;
        this.value = null;
        this.selectionKey = null;
        if(password.length > 0)
            this.password = password[0];
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

    /**
     * @return a metadata for system key range,
     * null if not metadata is associated.
     */
    public Metadata getMetadata() {return metadata;}

    /**
     * @return password for system key range,
     * null if not password is associated.
     */
    public String getPassword() { return password;}


}
