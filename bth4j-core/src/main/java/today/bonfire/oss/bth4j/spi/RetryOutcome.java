package today.bonfire.oss.bth4j.spi;

/** Result of {@link AdminStore#retry}. */
public enum RetryOutcome {
    /** the task is QUEUED again */
    RETRIED,
    /** the revision did not match, the task is not DEAD, CANCELLED or EXPIRED, or it does not exist */
    STALE,
    /**
     * the task's unique key is held by another task; nothing changed. A store that lets only one task record
     * carry a key at a time (PostgreSQL, SQLite) never returns it.
     */
    DUPLICATE
}
