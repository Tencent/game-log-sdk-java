package com.tencent.tglog;

/**
 * Exception thrown by TGLog operations.
 */
public class TglogException extends Exception {

    private final TglogErrorCode errorCode;

    /**
     * Create exception with error code.
     * @param errorCode Error code
     */
    public TglogException(TglogErrorCode errorCode) {
        super(errorCode.getMessage());
        this.errorCode = errorCode;
    }

    /**
     * Create exception with error code and custom message.
     * @param errorCode Error code
     * @param message Custom error message
     */
    public TglogException(TglogErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Create exception with error code and cause.
     * @param errorCode Error code
     * @param cause Cause of the exception
     */
    public TglogException(TglogErrorCode errorCode, Throwable cause) {
        super(errorCode.getMessage(), cause);
        this.errorCode = errorCode;
    }

    /**
     * Create exception with error code, custom message, and cause.
     * @param errorCode Error code
     * @param message Custom error message
     * @param cause Cause of the exception
     */
    public TglogException(TglogErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Create exception with message (error code defaults to UNKNOWN).
     * @param message Error message
     */
    public TglogException(String message) {
        this(TglogErrorCode.UNKNOWN, message);
    }

    /**
     * Create exception with message and cause (error code defaults to UNKNOWN).
     * @param message Error message
     * @param cause Cause of the exception
     */
    public TglogException(String message, Throwable cause) {
        this(TglogErrorCode.UNKNOWN, message, cause);
    }

    /**
     * Get the error code enum.
     * @return Error code
     */
    public TglogErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Get the numeric error code.
     * @return Numeric error code
     */
    public int getErrorCodeValue() {
        return errorCode.getCode();
    }

    @Override
    public String toString() {
        return String.format("TglogException: %s - %s", errorCode, getMessage());
    }
}
