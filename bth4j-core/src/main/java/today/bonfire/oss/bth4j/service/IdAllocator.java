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
            next = store.allocateIds(blockSize);
            end = next + blockSize;
        }
        return next++;
    }
}
