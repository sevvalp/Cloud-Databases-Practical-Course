package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage;

public class ServerMessage implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;

    public ServerMessage (StatusType status, String key, String value) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public ServerMessage (StatusType status, String key) {
        this.status = status;
        this.key = null;
        this.value = null;
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
