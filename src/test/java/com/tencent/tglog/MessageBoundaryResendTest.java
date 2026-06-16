package com.tencent.tglog;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the message-level boundary resend fix for bug #1.
 *
 * <p>When a TCP connection commits some bytes and then dies mid-write, the SDK must
 * resend at WHOLE-MESSAGE boundaries on the next connection:
 * <ul>
 *   <li>messages fully committed to the dead connection are notified as success and
 *       dropped (best-effort delivery on a request-only protocol);</li>
 *   <li>the single message straddling the commit boundary plus every message after it
 *       are kept and resent <em>intact</em> — never split across two connections;</li>
 *   <li>the remaining buffers are rewound so a resend transmits each message whole.</li>
 * </ul>
 */
public class MessageBoundaryResendTest {

    private static final int MSG_LEN = 10; // 9 'A' + '\n'

    private TglogConfig tcpConfig() {
        TglogConfig cfg = TglogConfig.createTcp("127.0.0.1", 12345);
        // Keep batch limits generous so add() never rejects our messages.
        cfg.setBatchingMaxMessages(1000);
        cfg.setBatchingMaxSize(1024 * 1024);
        return cfg;
    }

    /** Builds a payload of exactly MSG_LEN bytes ending in '\n', tagged with an index. */
    private String payload(int i) {
        StringBuilder sb = new StringBuilder();
        // 9 content bytes (last char before '\n') encode the index so we can identify it.
        sb.append(String.format("%09d", i));
        sb.append('\n');
        return sb.toString();
    }

    private BatchBuffer buildBatch(TglogConfig cfg, int count, List<String> succeeded, List<String> failed) {
        BatchBuffer batch = new BatchBuffer(cfg);
        for (int i = 0; i < count; i++) {
            final String p = payload(i);
            assertEquals(MSG_LEN, p.getBytes(StandardCharsets.UTF_8).length);
            batch.add(new LogMessage(p, (payload, ex) -> {
                if (ex == null) {
                    succeeded.add(payload);
                } else {
                    failed.add(payload);
                }
            }));
        }
        return batch;
    }

    @Test
    public void committedWholeMessagesDeliveredRemainderResentWhole() {
        TglogConfig cfg = tcpConfig();
        List<String> succeeded = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        final int count = 10;
        BatchBuffer batch = buildBatch(cfg, count, succeeded, failed);

        ByteBuffer[] buffers = batch.prepareForSend();
        assertEquals(count, buffers.length);

        // Simulate: the dead connection committed 3.5 messages worth of bytes
        // (35 bytes = 3 whole messages + 5 bytes of the 4th). Mirror the buffer
        // mutation a real partial gathering-write would cause so advancePastCommitted
        // works on realistically-positioned buffers.
        long committed = 3 * MSG_LEN + 5;
        consume(buffers, committed);

        int delivered = batch.advancePastCommitted(committed);

        // Exactly the 3 FULLY-committed messages are reported delivered.
        assertEquals(3, delivered);
        assertEquals(3, succeeded.size());
        assertEquals(payload(0), succeeded.get(0));
        assertEquals(payload(1), succeeded.get(1));
        assertEquals(payload(2), succeeded.get(2));
        // The straddling 4th message is NOT failed — it will be resent whole.
        assertTrue(failed.isEmpty());

        // The batch now holds messages 3..9 (7 messages), buffers rewound to full.
        assertEquals(7, batch.size());
        ByteBuffer[] resend = batch.prepareForSend();
        assertEquals(7, resend.length);
        for (int i = 0; i < resend.length; i++) {
            assertEquals("resend buffer must be rewound to message start", 0, resend[i].position());
            assertEquals("resend buffer must carry a whole message", MSG_LEN, resend[i].remaining());
        }

        // The straddling message (index 3) is resent intact, in full, at the front.
        byte[] first = new byte[MSG_LEN];
        resend[0].duplicate().get(first);
        assertEquals(payload(3), new String(first, StandardCharsets.UTF_8));
    }

    @Test
    public void exactBoundaryCommitDropsOnlyCompleteMessages() {
        TglogConfig cfg = tcpConfig();
        List<String> succeeded = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        BatchBuffer batch = buildBatch(cfg, 5, succeeded, failed);
        ByteBuffer[] buffers = batch.prepareForSend();

        // Commit exactly 2 whole messages — no straddling fragment.
        long committed = 2 * MSG_LEN;
        consume(buffers, committed);

        int delivered = batch.advancePastCommitted(committed);

        assertEquals(2, delivered);
        assertEquals(2, succeeded.size());
        assertTrue(failed.isEmpty());
        assertEquals(3, batch.size());
    }

    @Test
    public void zeroCommittedResendsWholeBatch() {
        TglogConfig cfg = tcpConfig();
        List<String> succeeded = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        BatchBuffer batch = buildBatch(cfg, 4, succeeded, failed);
        ByteBuffer[] buffers = batch.prepareForSend();
        // Buffers untouched (write threw before committing anything).

        int delivered = batch.advancePastCommitted(0);

        assertEquals(0, delivered);
        assertTrue(succeeded.isEmpty());
        assertTrue(failed.isEmpty());
        assertEquals(4, batch.size());
        ByteBuffer[] resend = batch.prepareForSend();
        assertEquals(4, resend.length);
        for (ByteBuffer b : resend) {
            assertEquals(0, b.position());
            assertEquals(MSG_LEN, b.remaining());
        }
    }

    @Test
    public void fullyCommittedBatchEmptiesAndDeliversAll() {
        TglogConfig cfg = tcpConfig();
        List<String> succeeded = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        BatchBuffer batch = buildBatch(cfg, 6, succeeded, failed);
        ByteBuffer[] buffers = batch.prepareForSend();

        long committed = 6 * MSG_LEN;
        consume(buffers, committed);

        int delivered = batch.advancePastCommitted(committed);

        assertEquals(6, delivered);
        assertEquals(6, succeeded.size());
        assertTrue(failed.isEmpty());
        assertTrue(batch.isEmpty());
    }

    @Test
    public void straddleAtFirstMessageResendsAll() {
        TglogConfig cfg = tcpConfig();
        List<String> succeeded = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        BatchBuffer batch = buildBatch(cfg, 4, succeeded, failed);
        ByteBuffer[] buffers = batch.prepareForSend();

        // Only a fragment of the first message was committed (3 of 10 bytes).
        long committed = 3;
        consume(buffers, committed);

        int delivered = batch.advancePastCommitted(committed);

        // No whole message committed -> nothing delivered, whole batch resent.
        assertEquals(0, delivered);
        assertTrue(succeeded.isEmpty());
        assertTrue(failed.isEmpty());
        assertEquals(4, batch.size());
        ByteBuffer[] resend = batch.prepareForSend();
        assertEquals(0, resend[0].position());
        assertEquals(MSG_LEN, resend[0].remaining());
        byte[] first = new byte[MSG_LEN];
        resend[0].duplicate().get(first);
        assertEquals(payload(0), new String(first, StandardCharsets.UTF_8));
    }

    /**
     * Advance buffer positions by {@code bytes} across the array, exactly as a partial
     * gathering write would. Asserts the array holds at least {@code bytes}.
     */
    private static void consume(ByteBuffer[] buffers, long bytes) {
        long left = bytes;
        for (ByteBuffer b : buffers) {
            if (left <= 0) {
                break;
            }
            int step = (int) Math.min(left, b.remaining());
            b.position(b.position() + step);
            left -= step;
        }
        assertFalse("test setup: not enough bytes in batch to consume " + bytes, left > 0);
    }
}
