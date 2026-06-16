package com.tencent.tglog;

import java.io.IOException;

/**
 * Thrown by {@link TglogConnection#send(java.nio.ByteBuffer[])} when the underlying
 * channel write committed some bytes to the kernel (or this connection's output
 * buffer) and then failed.
 *
 * <p>The protocol is request-only (no application ACK), so once bytes leave the SDK
 * on a connection that subsequently dies we cannot know how many actually reached the
 * peer. {@code committedBytes} is the number of bytes that this connection accepted
 * before the failure. The worker rounds this <em>down</em> to a whole-message boundary
 * (see {@link BatchBuffer#calculateSentMessages(long)}): messages fully below the
 * boundary are treated as best-effort delivered on the dead connection, the single
 * message straddling the boundary is considered torn (failed), and everything after it
 * is resent intact on the next connection. This guarantees the SDK never emits half a
 * message across two connections.
 */
class PartialWriteException extends IOException {

    private static final long serialVersionUID = 1L;

    /** Bytes this connection committed (kernel + outputBuffer) before failing. */
    private final long committedBytes;

    PartialWriteException(long committedBytes, Throwable cause) {
        super("partial write committed " + committedBytes + " bytes before failure", cause);
        this.committedBytes = committedBytes;
    }

    long getCommittedBytes() {
        return committedBytes;
    }
}
