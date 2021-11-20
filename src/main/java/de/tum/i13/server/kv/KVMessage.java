package de.tum.i13.server.kv;

import java.util.Locale;

public interface KVMessage {

    public enum StatusType {
        GET, 			/* Get - request */
        GET_ERROR, 		/* requested tuple (i.e. value) not found */
        GET_SUCCESS, 	/* requested tuple (i.e. value) found */
        PUT, 			/* Put - request */
        PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
        PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
        PUT_ERROR, 		/* Put - request not successful */
        DELETE, 		/* Delete - request */
        DELETE_SUCCESS, /* Delete - request successful */
        DELETE_ERROR, 	/* Delete - request successful */
        ERROR
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    public String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    public String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public StatusType getStatus();

    /**
     * @param status String of a status
     * @return Status associated with string, null if not found
     */
    public static StatusType parseStatus(String status) {
        for (StatusType t : StatusType.values()) {
            if (status.toUpperCase().equals(t.name().toUpperCase()))
                    return t;
        }

        return null;
    }

}
