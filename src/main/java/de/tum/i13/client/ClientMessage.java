package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.shared.Metadata;

public class ClientMessage implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;
    private Metadata metadata;

    public ClientMessage(StatusType status, String key, String value) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.metadata = null;
    }

    public ClientMessage(StatusType status, Metadata metadata) {
        this.status = status;
        this.metadata = metadata;
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

    /**
     * @return a metadata for system key range,
     * null if not metadata is associated.
     */
    public Metadata getMetadata() {return metadata;}
}
