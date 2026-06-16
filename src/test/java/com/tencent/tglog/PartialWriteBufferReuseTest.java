package com.tencent.tglog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reproduces bug #1: TCP partial write advances the caller-supplied
 * ByteBuffer positions. If those same buffers are reused to retry the
 * batch on a different connection (the sendBatchTcp connection-switch
 * path), the already-advanced bytes are NOT re-sent on the new
 * connection, while the first connection's outputBuffer also still
 * holds the remainder — leading to lost / duplicated data.
 *
 * <p>This test drives the real {@link TglogConnection#send(ByteBuffer[])}
 * against a real local TCP peer whose receive window is forced tiny and
 * which never reads, so {@code SocketChannel.write(buffers)} performs a
 * genuine partial write.
 */
public class PartialWriteBufferReuseTest {

    private ServerSocketChannel server;
    private Selector selector;
    private final List<SocketChannel> accepted = new ArrayList<>();

    @Before
    public void setUp() throws IOException {
        server = ServerSocketChannel.open();
        // Tiny receive buffer on the server side so the kernel TCP window is
        // small; the server never read()s, so the client socket send buffer
        // fills quickly and write() returns a partial count.
        server.setOption(StandardSocketOptions.SO_RCVBUF, 2048);
        server.bind(new InetSocketAddress("127.0.0.1", 0));
        // Non-blocking accept so the connect() helper never blocks waiting for a
        // peer that is not (yet) connecting.
        server.configureBlocking(false);
        selector = Selector.open();
    }

    @After
    public void tearDown() throws IOException {
        for (SocketChannel ch : accepted) {
            try { ch.close(); } catch (IOException ignored) { }
        }
        if (server != null) server.close();
        if (selector != null) selector.close();
    }

    private InetSocketAddress endpoint() {
        return new InetSocketAddress("127.0.0.1", server.socket().getLocalPort());
    }

    private TglogConfig tcpConfig() {
        TglogConfig cfg = TglogConfig.createTcp("127.0.0.1", server.socket().getLocalPort());
        // Force a tiny client send buffer to provoke partial writes deterministically.
        cfg.setSocketSendBufferSize(2048);
        cfg.setSocketRecvBufferSize(2048);
        // Make sure the high watermark is large so the path under test is the
        // partial-write buffering path, not the watermark-reject path.
        cfg.setWriteHighWatermark(64 * 1024 * 1024);
        return cfg;
    }

    private TglogConnection connect(TglogConfig cfg) throws IOException, InterruptedException {
        TglogMetrics metrics = new TglogMetrics(new io.prometheus.client.CollectorRegistry());
        TglogConnection conn = new TglogConnection(endpoint(), cfg, metrics, 0);
        conn.connect(selector);

        // Drive the selector until OP_CONNECT completes and accept on server side.
        long deadline = System.currentTimeMillis() + 5000;
        while (conn.getState() != TglogConnection.State.CONNECTED
                && System.currentTimeMillis() < deadline) {
            selector.select(100);
            for (SelectionKey key : selector.selectedKeys()) {
                if (key.isConnectable()) {
                    ((TglogConnection) key.attachment()).finishConnect();
                }
            }
            selector.selectedKeys().clear();
            SocketChannel s = server.accept();
            if (s != null) {
                s.configureBlocking(true);
                accepted.add(s);
            }
        }
        if (conn.getState() != TglogConnection.State.CONNECTED) {
            fail("connection did not reach CONNECTED state");
        }
        // Accept any straggler.
        SocketChannel s = server.accept();
        if (s != null) {
            s.configureBlocking(true);
            accepted.add(s);
        }
        return conn;
    }

    /**
     * Build the exact buffer array a worker would hand to
     * {@link TglogConnection#send(ByteBuffer[])} and later reuse on retry:
     * one {@code ByteBuffer.wrap(payloadBytes)} per message, mirroring
     * {@code BatchBuffer.prepareForTcp()}.
     *
     * <p>We construct it directly (rather than via {@code BatchBuffer.add})
     * because BatchBuffer intentionally caps a batch at batchingMaxSize, and
     * we want a payload large enough to guarantee a partial socket write. The
     * per-message {@code ByteBuffer.wrap} is identical to what BatchBuffer
     * produces, so the partial-write / reuse semantics under test are exact.
     */
    private ByteBuffer[] buildBatch(TglogConfig cfg, int messageCount, int messageBytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageBytes - 1; i++) {
            sb.append('A');
        }
        sb.append('\n');
        byte[] payloadBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer[] buffers = new ByteBuffer[messageCount];
        for (int i = 0; i < messageCount; i++) {
            buffers[i] = ByteBuffer.wrap(payloadBytes);
        }
        return buffers;
    }

    @Test
    public void partialWriteAdvancesBuffersAndReuseLosesData() throws Exception {
        TglogConfig cfg = tcpConfig();
        TglogConnection conn1 = connect(cfg);

        // ~512 KB total: comfortably larger than the 2KB socket buffers, so the
        // first write() to a non-reading peer is guaranteed to be partial.
        final int msgCount = 256;
        final int msgBytes = 2048;
        ByteBuffer[] buffers = buildBatch(cfg, msgCount, msgBytes);

        long totalToSend = 0;
        for (ByteBuffer b : buffers) {
            totalToSend += b.remaining();
        }

        // Send to the first connection. The peer never reads, so this must be a
        // partial write: TglogConnection reports the whole batch as "accepted"
        // (returns totalToSend) and copies the unsent remainder into its
        // internal outputBuffer.
        long accepted1 = conn1.send(buffers);
        assertEquals("send() claims the whole batch was accepted", totalToSend, accepted1);

        long bufferedOnConn1 = conn1.getBufferedDataSize();

        // A genuine partial write occurred: part went to the kernel, the unsent
        // remainder was copied into conn1's internal outputBuffer.
        assertTrue("expected a partial write (some bytes buffered on conn1)", bufferedOnConn1 > 0);
        assertNotEquals("expected partial, not full, write", totalToSend, bufferedOnConn1);

        // ---- The bug ----
        // send() FULLY DRAINS every input buffer: the written prefix was consumed
        // by the kernel, and the unsent remainder was drained via copy.put(buf)
        // into conn1's outputBuffer. So after a "successful" partial send, the
        // caller's buffers have ZERO bytes remaining.
        long remainingAfterFirstSend = 0;
        for (ByteBuffer b : buffers) {
            remainingAfterFirstSend += b.remaining();
        }
        assertEquals(
                "BUG: send() drained ALL input buffers (prefix consumed by kernel, "
                        + "remainder copied into conn1.outputBuffer), leaving nothing to resend",
                0, remainingAfterFirstSend);

        // sendBatchTcp returns OK on a partial write, so normally these buffers
        // are not reused. But on the connection-switch retry path (a later
        // sent<0 / sent==0 on a DIFFERENT attempt within the same batch), the
        // worker reuses this very same buffers[] array against a new connection.
        // prepareForSend()'s rewind() is only applied on the next retry CYCLE,
        // not within a single sendBatchTcp() connection switch — and even rewind()
        // would over-send because the prefix is already on conn1's wire.
        //
        // Demonstrate the concrete data loss: reusing the (now-empty) buffers on
        // a fresh connection transmits ZERO bytes, while the entire unsent
        // remainder is stranded inside conn1.outputBuffer — and conn1 is exactly
        // the connection the worker is abandoning, so that data may never flush.
        TglogConnection conn2 = connect(cfg);
        long accepted2 = conn2.send(buffers);
        assertEquals(
                "DATA LOSS: reusing drained buffers on conn2 sends nothing; the whole "
                        + "remainder is trapped in the abandoned conn1's outputBuffer",
                0, accepted2);

        conn1.close(); // closing conn1 discards its outputBuffer -> remainder lost for good
        conn2.close();
    }

    /**
     * Companion check: prepareForSend()'s rewind() on retry does NOT fix the
     * problem after notifyAndRemove() has trimmed some buffers, because the
     * buffer that straddles the sent/unsent boundary keeps its advanced
     * position relative to what was actually written.
     *
     * <p>Here we show the simpler, foundational fact the worker relies on:
     * after a partial write through send(), the buffers are mutated, so the
     * contract "buffers can be safely re-sent as-is" is already violated.
     */
    @Test
    public void buffersAreMutatedBySendBreakingResendContract() throws Exception {
        TglogConfig cfg = tcpConfig();
        TglogConnection conn = connect(cfg);

        ByteBuffer[] buffers = buildBatch(cfg, 256, 2048);

        // Snapshot original positions/limits.
        int[] originalPos = new int[buffers.length];
        for (int i = 0; i < buffers.length; i++) {
            originalPos[i] = buffers[i].position();
        }

        conn.send(buffers);

        boolean anyAdvanced = false;
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i].position() != originalPos[i]) {
                anyAdvanced = true;
                break;
            }
        }
        assertTrue("send() advanced at least one buffer's position (mutation)", anyAdvanced);

        conn.close();
    }
}
