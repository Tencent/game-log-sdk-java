package com.tencent.tglog;

/**
 * Callback interface for asynchronous log sending.
 */
public interface TglogCallback {

    /**
     * Called when the log is successfully sent.
     *
     * @param payload The log payload that was sent
     */
    void onSuccess(String payload);

    /**
     * Called when the log fails to send.
     *
     * @param payload The log payload that failed
     * @param errorCode Error code
     * @param errorMessage Error message
     */
    void onError(String payload, int errorCode, String errorMessage);
}
