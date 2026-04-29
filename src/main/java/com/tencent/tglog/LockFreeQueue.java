package com.tencent.tglog;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Lock-free bounded MPSC (Multi-Producer Single-Consumer) queue.
 *
 * This is a high-performance queue implementation using CAS operations,
 * similar to the ConcurrencyKit ring buffer used in the C version.
 *
 * Design:
 * - Multiple producers can enqueue concurrently without locks
 * - Single consumer dequeues messages
 * - Fixed capacity with power-of-2 size for efficient modulo
 * - Uses atomic operations for thread-safety
 * - Cache line padding prevents false sharing between writeIndex and readIndex
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

        long currentWrite;
        long nextWrite;

        do {
            currentWrite = writeIndex.get();
            nextWrite = currentWrite + 1;

            // Check if queue is full
            long currentRead = readIndex.get();
            if (nextWrite - currentRead > capacity) {
                return false;
            }

            // Try to claim this slot
        } while (!writeIndex.compareAndSet(currentWrite, nextWrite));

        // We've claimed the slot, now write the item
        int index = (int) (currentWrite & mask);
        buffer.set(index, item);

        return true;
    }

    /**
     * Dequeue an item (non-blocking).
     * Should only be called by a single consumer thread.
     *
     * @return The dequeued item, or null if queue is empty
     */
    public T poll() {
        long currentRead = readIndex.get();
        long currentWrite = writeIndex.get();

        // Check if queue is empty
        if (currentRead >= currentWrite) {
            return null;
        }

        int index = (int) (currentRead & mask);
        T item = buffer.get(index);

        // Item might not be written yet by producer
        if (item == null) {
            return null;
        }

        // Clear the slot and advance read index
        buffer.set(index, null);
        readIndex.lazySet(currentRead + 1);

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
     * Round up to the next power of 2.
     */
    private static int roundUpToPowerOf2(int value) {
        if (value <= 0) {
            return 1;
        }
        if ((value & (value - 1)) == 0) {
            return value; // Already power of 2
        }
        int power = 1;
        while (power < value) {
            power <<= 1;
        }
        return power;
    }
}
