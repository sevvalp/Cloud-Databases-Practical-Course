package de.tum.i13.server.kv;

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
        DELETE_ERROR 	/* Delete - request successful */
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
        switch (status) {
            case "GET": return StatusType.GET;
            case "GET_ERROR": return StatusType.GET_ERROR;
            case "GET_SUCCESS": return StatusType.GET_SUCCESS;
            case "PUT": return StatusType.PUT;
            case "PUT_SUCCESS": return StatusType.PUT_SUCCESS;
            case "PUT_UPDATE": return StatusType.PUT_UPDATE;
            case "PUT_ERROR": return StatusType.PUT_ERROR;
            case "DELETE": return StatusType.DELETE;
            case "DELETE_SUCCESS": return StatusType.DELETE_SUCCESS;
            case "DELETE_ERROR": return StatusType.DELETE_ERROR;
            default: return null;
        }
    }

}
