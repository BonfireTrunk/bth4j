package today.bonfire.oss.bth4j.service;

import java.util.concurrent.locks.ReentrantLock;
import today.bonfire.oss.bth4j.spi.TaskStore;

/**
 * Hands out task ids from blocks reserved in the store, one store call per block. Ids from one process
 * are increasing; across processes they interleave.
 */
public final class IdAllocator {

    private final TaskStore store;
    private final int blockSize;
    private final ReentrantLock lock = new ReentrantLock();
    private long next;
    private long end;

    public IdAllocator(TaskStore store, int blockSize) {
        this.store = store;
        this.blockSize = Math.max(1, blockSize);
    }

    public long nextId() {
        lock.lock();
        try {
            if (next >= end) {
                reserve(blockSize);
            }
            return next++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reserves {@code count} consecutive ids and returns the first. They come from the current block when it
     * has room, otherwise from one new block of at least {@code count} ids; the rest of the old block is
     * abandoned, which leaves a gap in the sequence and nothing else.
     */
    public long nextIds(int count) {
        lock.lock();
        try {
            if (count < 1) {
                throw new IllegalArgumentException("count must be positive");
            }
            if (end - next < count) {
                reserve(Math.max(count, blockSize));
            }
            var first = next;
            next += count;
            return first;
        } finally {
            lock.unlock();
        }
    }

    /** Discards unhanded ids, so collision recovery starts with a newly allocated store block. */
    public void abandonBlock() {
        lock.lock();
        try {
            next = end;
        } finally {
            lock.unlock();
        }
    }

    /** The exclusive end of the last block allocated by this client; every smaller positive id was allocated. */
    public long knownEnd() {
        lock.lock();
        try {
            return end;
        } finally {
            lock.unlock();
        }
    }

    private void reserve(int size) {
        next = store.allocateIds(size);
        end = Math.addExact(next, size);
    }
}
