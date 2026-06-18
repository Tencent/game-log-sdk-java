package com.tencent.tglog;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-concurrency stress test for {@link LockFreeQueue}.
 *
 * This is a NETWORK-FREE, self-contained correctness test. It hammers the queue
 * with many producer threads and a single consumer thread, then verifies the
 * invariants that the lock-free MPSC queue must uphold:
 *
 *   1. No loss        - every successfully offered item is eventually polled.
 *   2. No duplication  - each item is polled at most once.
 *   3. Per-producer FIFO - items from the same producer come out in send order
 *                          (MPSC consumer reads readIndex monotonically).
 *   4. Capacity bound  - the queue never holds more than `capacity` items, and
 *                        a full queue rejects offers (returns false) rather than
 *                        overwriting an unconsumed slot.
 *
 * Each item is encoded as a single long: (producerId << 40) | sequence. This
 * lets the consumer reconstruct per-producer ordering and detect duplicates
 * cheaply without object allocation in the hot path.
 *
 * Run after compilation, e.g.:
 *   java -cp "target/classes:target/test-classes:target/lib/*" \
 *        com.tencent.tglog.LockFreeQueueStressTest
 *   java -cp "..." com.tencent.tglog.LockFreeQueueStressTest 16 5000000 1024
 */
public final class LockFreeQueueStressTest {

    // Item encoding: top 24 bits producerId, low 40 bits sequence.
    private static final int SEQ_BITS = 40;
    private static final long SEQ_MASK = (1L << SEQ_BITS) - 1;

    private LockFreeQueueStressTest() {
    }

    public static void main(String[] args) throws Exception {
        int producers = args.length > 0 ? Integer.parseInt(args[0]) : 8;
        long perProducer = args.length > 1 ? Long.parseLong(args[1]) : 2_000_000L;
        int capacity = args.length > 2 ? Integer.parseInt(args[2]) : 1024;

        System.out.println("==================================================");
        System.out.println("LockFreeQueue Stress Test");
        System.out.println("==================================================");
        System.out.println("  Producer threads : " + producers);
        System.out.println("  Items / producer : " + String.format("%,d", perProducer));
        System.out.println("  Total items      : " + String.format("%,d", producers * perProducer));
        System.out.println("  Queue capacity   : " + capacity + " (rounded to pow2)");
        System.out.println("--------------------------------------------------");

        boolean ok = runCorrectness(producers, perProducer, capacity);
        ok &= runFullEmptyBehavior();

        System.out.println("==================================================");
        if (ok) {
            System.out.println("RESULT: PASS");
            System.exit(0);
        } else {
            System.out.println("RESULT: FAIL");
            System.exit(1);
        }
    }

    /**
     * Many producers, single consumer. Verifies no loss, no duplication and
     * per-producer FIFO ordering.
     */
    private static boolean runCorrectness(int producers, long perProducer, int capacity)
            throws InterruptedException {
        final LockFreeQueue<Long> queue = new LockFreeQueue<>(capacity);
        final long totalExpected = producers * perProducer;

        // Per-producer counters: items offered and the next sequence the
        // consumer expects to see (for FIFO verification).
        final long[] offered = new long[producers];
        final long[] expectedNextSeq = new long[producers];

        final AtomicLong polledCount = new AtomicLong(0);
        final AtomicBoolean orderViolation = new AtomicBoolean(false);
        final AtomicBoolean producersDone = new AtomicBoolean(false);

        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch producersFinished = new CountDownLatch(producers);

        // ---- Producer threads ----
        Thread[] producerThreads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            final int producerId = p;
            producerThreads[p] = new Thread(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                long localOffered = 0;
                for (long seq = 0; seq < perProducer; seq++) {
                    long encoded = ((long) producerId << SEQ_BITS) | seq;
                    // Spin until the bounded queue accepts the item. A full
                    // queue legitimately returns false; we must NOT drop.
                    while (!queue.offer(encoded)) {
                        // busy spin (Java 8 has no Thread.onSpinWait)
                    }
                    localOffered++;
                }
                offered[producerId] = localOffered;
                producersFinished.countDown();
            }, "producer-" + p);
            producerThreads[p].start();
        }

        // ---- Single consumer thread ----
        Thread consumer = new Thread(() -> {
            long seen = 0;
            while (true) {
                Long item = queue.poll();
                if (item == null) {
                    if (producersDone.get() && queue.isEmpty()) {
                        break;
                    }
                    // busy spin (Java 8 has no Thread.onSpinWait)
                    continue;
                }
                long v = item;
                int pid = (int) (v >>> SEQ_BITS);
                long seq = v & SEQ_MASK;
                if (seq != expectedNextSeq[pid]) {
                    // Out-of-order or duplicate/missing item for this producer.
                    orderViolation.set(true);
                    System.out.printf("  [ORDER] producer=%d expected seq=%d but got seq=%d%n",
                            pid, expectedNextSeq[pid], seq);
                }
                expectedNextSeq[pid] = seq + 1;
                seen++;
                polledCount.lazySet(seen);
            }
        }, "consumer");

        long start = System.nanoTime();
        consumer.start();
        startGate.countDown();

        producersFinished.await();
        producersDone.set(true);
        consumer.join(TimeUnit.SECONDS.toMillis(60));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        for (Thread t : producerThreads) {
            t.join();
        }

        long totalOffered = 0;
        for (long o : offered) {
            totalOffered += o;
        }
        long totalPolled = polledCount.get();

        System.out.println("Correctness test:");
        System.out.println("  Elapsed          : " + elapsedMs + " ms");
        if (elapsedMs > 0) {
            System.out.println("  Throughput       : "
                    + String.format("%,d", totalPolled * 1000 / elapsedMs) + " items/s");
        }
        System.out.println("  Total offered    : " + String.format("%,d", totalOffered));
        System.out.println("  Total polled     : " + String.format("%,d", totalPolled));
        System.out.println("  Expected total   : " + String.format("%,d", totalExpected));

        boolean consumerAlive = consumer.isAlive();
        if (consumerAlive) {
            System.out.println("  [FAIL] consumer did not drain within 60s "
                    + "(possible lost items / deadlock)");
            consumer.interrupt();
        }

        boolean lossless = (totalPolled == totalExpected) && (totalOffered == totalExpected);
        boolean inOrder = !orderViolation.get();

        System.out.println("  No loss/dup      : " + (lossless ? "PASS" : "FAIL"));
        System.out.println("  Per-producer FIFO: " + (inOrder ? "PASS" : "FAIL"));
        System.out.println("--------------------------------------------------");

        return lossless && inOrder && !consumerAlive;
    }

    /**
     * Single-threaded check of capacity rounding, the capacity bound, and
     * full/empty transitions.
     */
    private static boolean runFullEmptyBehavior() {
        boolean ok = true;
        int[] requestedValues = {0, 1, 2, 3, 7, 8, 9};
        for (int requested : requestedValues) {
            ok &= runFullEmptyBehavior(requested);
        }

        boolean tooLargeRejected = false;
        try {
            new LockFreeQueue<Integer>((1 << 30) + 1);
        } catch (IllegalArgumentException expected) {
            tooLargeRejected = true;
        }
        if (!tooLargeRejected) {
            System.out.println("  [FAIL] oversized capacity should be rejected");
            ok = false;
        }

        System.out.println("Full/empty + wrap-around test: " + (ok ? "PASS" : "FAIL"));
        System.out.println("--------------------------------------------------");
        return ok;
    }

    private static boolean runFullEmptyBehavior(int requested) {
        LockFreeQueue<Integer> q = new LockFreeQueue<>(requested);
        int cap = q.capacity();
        boolean ok = true;

        if (cap < 2 || (cap & (cap - 1)) != 0 || (requested > 2 && cap < requested)) {
            System.out.printf("  [FAIL] requested=%d rounded to invalid capacity=%d%n", requested, cap);
            ok = false;
        }

        // Fill exactly to capacity.
        for (int i = 0; i < cap; i++) {
            if (!q.offer(i)) {
                System.out.printf("  [FAIL] requested=%d offer rejected before full at i=%d%n",
                        requested, i);
                ok = false;
            }
        }
        if (!q.isFull()) {
            System.out.printf("  [FAIL] requested=%d queue should report full%n", requested);
            ok = false;
        }
        // One past capacity must be rejected (no overwrite).
        if (q.offer(999)) {
            System.out.printf("  [FAIL] requested=%d offer accepted past capacity%n", requested);
            ok = false;
        }
        if (q.size() != cap) {
            System.out.printf("  [FAIL] requested=%d size=%d expected=%d%n", requested, q.size(), cap);
            ok = false;
        }

        // Drain and verify exact FIFO contents 0..cap-1.
        for (int i = 0; i < cap; i++) {
            Integer v = q.poll();
            if (v == null || v != i) {
                System.out.printf("  [FAIL] requested=%d drain mismatch at %d: got %s%n",
                        requested, i, v);
                ok = false;
            }
        }
        if (q.poll() != null) {
            System.out.printf("  [FAIL] requested=%d poll returned item from empty queue%n", requested);
            ok = false;
        }
        if (!q.isEmpty()) {
            System.out.printf("  [FAIL] requested=%d queue should report empty%n", requested);
            ok = false;
        }

        // Wrap-around: refill and drain again to exercise sequence reuse one lap later.
        for (int i = 0; i < cap; i++) {
            if (!q.offer(1000 + i)) {
                System.out.printf("  [FAIL] requested=%d wrap refill rejected at i=%d%n", requested, i);
                ok = false;
            }
        }
        for (int i = 0; i < cap; i++) {
            Integer v = q.poll();
            if (v == null || v != 1000 + i) {
                System.out.printf("  [FAIL] requested=%d wrap drain mismatch at %d: got %s%n",
                        requested, i, v);
                ok = false;
            }
        }

        return ok;
    }
}
