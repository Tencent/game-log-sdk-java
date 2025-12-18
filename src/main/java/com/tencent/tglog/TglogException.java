package com.tencent.tglog;

/**
 * Exception thrown by TGLog operations.
 */
public class TglogException extends Exception {

    private final int errorCode;

    public TglogException(String message) {
        this(message, -1);
    }

    public TglogException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public TglogException(String message, Throwable cause) {
        this(message, -1, cause);
    }

    public TglogException(String message, int errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    // Error codes (aligned with C version)
    public static final int ERR_OK = 0;
    public static final int ERR_SEND_TIMEOUT = 10001;
    public static final int ERR_SEND_FAILED = 10002;
    public static final int ERR_PRODUCER_CLOSED = 10003;
    public static final int ERR_SEND_QUEUE_IS_FULL = 10004;
    public static final int ERR_NO_AVAILABLE_CONN = 10005;
    public static final int ERR_NEW_CONN_FAILED = 10006;
    public static final int ERR_CONN_WRITE_FAILED = 10007;
    public static final int ERR_CONN_READ_FAILED = 10008;
    public static final int ERR_LOG_TOO_LONG = 10009;
    public static final int ERR_BAD_LOG = 10010;
    public static final int ERR_BATCH_SIZE_EXCEEDED = 10011;
    public static final int ERR_MEMORY_ALLOC_FAILED = 10012;
    public static final int ERR_WORKER_BUSY = 10013;
    public static final int ERR_UNKNOWN = 20001;
}
