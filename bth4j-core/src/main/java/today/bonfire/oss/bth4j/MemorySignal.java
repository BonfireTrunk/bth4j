package today.bonfire.oss.bth4j;

/** Memory pressure used by adaptive concurrency. */
public enum MemorySignal {
    /** Heap usage relative to its maximum, or committed size when the maximum is undefined. */
    HEAP,
    /**
     * The greater of heap and system usage, including native memory. The system figure can count OS file
     * cache as used and hold concurrency low on a long-running host.
     */
    HEAP_AND_SYSTEM
}
