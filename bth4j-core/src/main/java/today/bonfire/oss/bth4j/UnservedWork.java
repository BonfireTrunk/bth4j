package today.bonfire.oss.bth4j;

/**
 * Waiting work without a live accepting runner. A null type means the whole queue is unserved and waiting is its
 * queued count; otherwise waiting is the exact queued count of that type.
 */
public record UnservedWork(String queue, String type, long waiting) {}
