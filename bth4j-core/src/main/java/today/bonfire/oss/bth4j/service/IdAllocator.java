package today.bonfire.oss.bth4j.service;

import today.bonfire.oss.bth4j.spi.TaskStore;

/**
 * Hands out task ids from blocks reserved in the store, one store call per block. Ids from one process
 * are increasing; across processes they interleave.
 */
public final class IdAllocator {

    private final TaskStore store;
    private final int blockSize;
    private long next;
    private long end;

    public IdAllocator(TaskStore store, int blockSize) {
        this.store = store;
        this.blockSize = Math.max(1, blockSize);
    }

    public synchronized long nextId() {
        if (next >= end) {
            reserve(blockSize);
        }
        return next++;
    }

    /**
     * Reserves {@code count} consecutive ids and returns the first. They come from the current block when it
     * has room, otherwise from one new block of at least {@code count} ids; the rest of the old block is
     * abandoned, which leaves a gap in the sequence and nothing else.
     */
    public synchronized long nextIds(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be positive");
        }
        if (end - next < count) {
            reserve(Math.max(count, blockSize));
        }
        var first = next;
        next += count;
        return first;
    }

    private void reserve(int size) {
        next = store.allocateIds(size);
        end = Math.addExact(next, size);
    }
}
