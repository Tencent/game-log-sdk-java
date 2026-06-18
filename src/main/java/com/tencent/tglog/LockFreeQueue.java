package com.tencent.tglog;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Lock-free bounded MPSC (Multi-Producer Single-Consumer) queue.
 *
 * This implementation uses the Disruptor-style per-slot sequence pattern so that
 * the consumer never has to guess whether a slot is "empty" or merely "claimed
 * but not yet filled". Each slot carries a sequence number that the producer
 * publishes (with release semantics) only AFTER the item has been stored. The
 * consumer reads the sequence (with acquire semantics) and only consumes a slot
 * whose sequence matches the expected value.
 *
 * Why the previous CAS-claim-then-write design was unsafe:
 * - A producer would CAS writeIndex first and write the item afterwards. That
 *   left a window where writeIndex had advanced but the slot was still null.
 * - The consumer used "slot == null" to decide the queue was empty, which is
 *   indistinguishable from a slot that is genuinely empty. A single slow
 *   producer could therefore stall delivery of every message behind it
 *   (head-of-line blocking), and the size/full checks relied on cross-thread
 *   index visibility that lazySet does not guarantee.
 *
 * Design:
 * - Multiple producers enqueue concurrently without locks (CAS on writeIndex).
 * - A single consumer dequeues messages.
 * - Fixed power-of-2 capacity for cheap masking.
 * - Per-slot sequence provides the publication/visibility handshake.
 * - Cache-line padding prevents false sharing between writeIndex and readIndex.
 */
class LockFreeQueue<T> {

    /**
     * Padded AtomicLong: each instance occupies its own 64-byte cache line.
     *
     * Layout (object header ~16 bytes + value 8 bytes + 6 longs * 8 = 64 bytes):
     *   [object header 16B][value 8B][p1-p6 48B] = 72B >= 64B, guaranteed isolated.
     */
    @SuppressWarnings("unused")
    private static class PaddedAtomicLong extends AtomicLong {
        // 6 longs of trailing padding (48 bytes) so that the value field of
        // AtomicLong (8 bytes) plus the object header (~16 bytes) never shares
        // a 64-byte cache line with any adjacent field.
        private long p1, p2, p3, p4, p5, p6;

        PaddedAtomicLong(long initialValue) {
            super(initialValue);
        }
    }

    private final int capacity;
    private final int mask;
    private final AtomicReferenceArray<T> buffer;
    // Per-slot publication sequence. A slot at array index i is "ready to be
    // consumed for ticket number n" iff sequence[i] == n + 1.
    // Initialized so that slot i is ready to be CLAIMED (written) for ticket i.
    private final AtomicLongArray sequence;
    // Each index lives on its own cache line to avoid false sharing between
    // producer threads (writeIndex) and the consumer thread (readIndex).
    private final PaddedAtomicLong writeIndex = new PaddedAtomicLong(0);
    private final PaddedAtomicLong readIndex = new PaddedAtomicLong(0);

    /**
     * Create a lock-free queue with the specified capacity.
     * Capacity will be rounded up to the next power of 2.
     *
     * @param capacity Desired capacity
     */
    public LockFreeQueue(int capacity) {
        // Round up to next power of 2
        this.capacity = roundUpToPowerOf2(capacity);
        this.mask = this.capacity - 1;
        this.buffer = new AtomicReferenceArray<>(this.capacity);
        this.sequence = new AtomicLongArray(this.capacity);
        // Slot i starts available for the producer holding ticket i.
        for (int i = 0; i < this.capacity; i++) {
            this.sequence.set(i, i);
        }
    }

    /**
     * Try to enqueue an item (non-blocking).
     *
     * @param item Item to enqueue
     * @return true if successful, false if queue is full
     */
    public boolean offer(T item) {
        if (item == null) {
            throw new NullPointerException("Cannot enqueue null item");
        }

        long ticket;
        for (;;) {
            ticket = writeIndex.get();
            int index = (int) (ticket & mask);
            // A slot is writable for this ticket only when its published
            // sequence equals the ticket value. get() on AtomicLongArray gives
            // volatile (acquire) semantics.
            long seq = sequence.get(index);
            long diff = seq - ticket;
            if (diff == 0) {
                // Slot is free for us; try to claim the ticket.
                if (writeIndex.compareAndSet(ticket, ticket + 1)) {
                    // We own this slot. Store the item, then publish the slot by
                    // bumping its sequence to ticket + 1. The set() below has
                    // release semantics, so the consumer that observes the new
                    // sequence is guaranteed to also observe the item.
                    buffer.lazySet(index, item);
                    sequence.set(index, ticket + 1);
                    return true;
                }
                // Lost the race; retry with a fresh ticket.
            } else if (diff < 0) {
                // The slot has not been consumed yet for the previous lap, i.e.
                // sequence < ticket means the consumer is still behind: full.
                return false;
            }
            // diff > 0: another producer already advanced past this ticket;
            // retry to pick up the latest writeIndex.
        }
    }

    /**
     * Dequeue an item (non-blocking).
     * Should only be called by a single consumer thread.
     *
     * @return The dequeued item, or null if queue is empty
     */
    public T poll() {
        long ticket = readIndex.get();
        int index = (int) (ticket & mask);
        // The slot is ready to be consumed only once its sequence has been
        // published to ticket + 1 by the producer. Any other value means the
        // slot is empty or not yet filled -> treat as empty.
        long seq = sequence.get(index);
        if (seq - (ticket + 1) != 0) {
            return null;
        }

        T item = buffer.get(index);
        buffer.lazySet(index, null);
        // Mark the slot free for the producer that will use it one lap later
        // (ticket + capacity). Release semantics publish the item-cleared state.
        sequence.set(index, ticket + capacity);
        // Single consumer, so a plain lazySet on readIndex is enough; producers
        // gate exclusively on per-slot sequence, not on readIndex.
        readIndex.lazySet(ticket + 1);
        return item;
    }

    /**
     * Get the current size of the queue (approximate).
     *
     * @return Approximate number of items in queue
     */
    public int size() {
        long write = writeIndex.get();
        long read = readIndex.get();
        return (int) Math.max(0, write - read);
    }

    /**
     * Check if queue is empty (approximate).
     *
     * @return true if queue appears empty
     */
    public boolean isEmpty() {
        return readIndex.get() >= writeIndex.get();
    }

    /**
     * Check if queue is full (approximate).
     *
     * @return true if queue appears full
     */
    public boolean isFull() {
        long write = writeIndex.get();
        long read = readIndex.get();
        return write - read >= capacity;
    }

    /**
     * Get the capacity of the queue.
     *
     * @return Queue capacity
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Round up to the next power of 2, with a minimum capacity of 2.
     */
    private static int roundUpToPowerOf2(int value) {
        if (value <= 2) {
            return 2;
        }
        if (value > (1 << 30)) {
            throw new IllegalArgumentException("Capacity too large");
        }

        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }
}
