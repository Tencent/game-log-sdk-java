package com.tencent.tglog;

/**
 * Error codes for TGLog operations.
 * Aligned with C version error codes.
 */
public enum TglogErrorCode {
    /** Operation succeeded */
    OK(0, "Success"),

    /** Send operation timed out */
    SEND_TIMEOUT(10001, "Send timeout"),

    /** Send operation failed */
    SEND_FAILED(10002, "Send failed"),

    /** Producer/client is closed */
    PRODUCER_CLOSED(10003, "Producer closed"),

    /** Message queue is full */
    SEND_QUEUE_IS_FULL(10004, "Send queue is full"),

    /** No available connections */
    NO_AVAILABLE_CONN(10005, "No available connections"),

    /** Failed to create new connection */
    NEW_CONN_FAILED(10006, "Failed to create new connection"),

    /** Connection write failed */
    CONN_WRITE_FAILED(10007, "Connection write failed"),

    /** Connection read failed */
    CONN_READ_FAILED(10008, "Connection read failed"),

    /** Log payload is too long */
    LOG_TOO_LONG(10009, "Log payload too long"),

    /** Invalid log payload */
    BAD_LOG(10010, "Invalid log payload"),

    /** Batch size exceeded */
    BATCH_SIZE_EXCEEDED(10011, "Batch size exceeded"),

    /** Memory allocation failed */
    MEMORY_ALLOC_FAILED(10012, "Memory allocation failed"),

    /** Worker is busy */
    WORKER_BUSY(10013, "Worker is busy"),

    /** No matching request for response */
    NO_MATCH_REQ_FOR_RSP(10014, "No matching request for response"),

    /** Connection closed by peer */
    CONN_CLOSED_BY_PEER(10015, "Connection closed by peer"),

    /** Connection EAGAIN error (socket buffer full) */
    CONN_EAGAIN(10016, "Connection EAGAIN error"),

    /** Bad flag */
    BAD_FLAG(10017, "Bad flag"),

    /** Unknown error */
    UNKNOWN(20001, "Unknown error");

    private final int code;
    private final String message;

    TglogErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * Get the numeric error code.
     * @return Error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Get the error message.
     * @return Error message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Convert numeric error code to enum.
     * @param code Numeric error code
     * @return Corresponding error code enum, or UNKNOWN if not found
     */
    public static TglogErrorCode fromCode(int code) {
        for (TglogErrorCode ec : values()) {
            if (ec.code == code) {
                return ec;
            }
        }
        return UNKNOWN;
    }

    /**
     * Check if this is a success code.
     * @return true if OK
     */
    public boolean isOk() {
        return this == OK;
    }

    /**
     * Check if this is an error code.
     * @return true if not OK
     */
    public boolean isError() {
        return this != OK;
    }

    @Override
    public String toString() {
        return String.format("%s(%d): %s", name(), code, message);
    }
}
