package today.bonfire.oss.bth4j;

/**
 * Waiting work without a live worker. A null type means the whole queue is unserved and waiting is its
 * queued count; otherwise waiting is a lower bound from a sample of up to 100 queued tasks.
 */
public record UnservedWork(String queue, String type, long waiting) {}
