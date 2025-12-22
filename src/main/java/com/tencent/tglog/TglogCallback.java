package com.tencent.tglog;

/**
 * Callback interface for asynchronous log sending.
 *
 * Similar to Kafka's Callback interface, this uses a single method
 * that is called on both success and failure. Check if exception is null
 * to determine success.
 *
 * <p>Example usage:
 * <pre>
 * // Lambda style
 * client.send("log\n", (payload, exception) -> {
 *     if (exception == null) {
 *         System.out.println("Success");
 *     } else {
 *         System.err.println("Failed: " + exception.getMessage());
 *     }
 * });
 *
 * // Anonymous class style
 * client.send("log\n", new TglogCallback() {
 *     public void onCompletion(String payload, TglogException exception) {
 *         if (exception == null) {
 *             // success
 *         } else {
 *             // failure
 *         }
 *     }
 * });
 * </pre>
 */
@FunctionalInterface
public interface TglogCallback {

    /**
     * Called when the send operation completes (either success or failure).
     *
     * @param payload The log payload that was sent
     * @param exception null if send succeeded, otherwise contains the error details
     */
    void onCompletion(String payload, TglogException exception);
}
