package today.bonfire.oss.bth4j.spi;

/** Result of {@link AdminStore#retry}. The store answers the first that applies, in the order listed. */
public enum RetryOutcome {
    /** the task is QUEUED again */
    RETRIED,
    /** no task has this id */
    NOT_FOUND,
    /** the revision did not match */
    STALE,
    /** the task is not DEAD, CANCELLED or EXPIRED */
    NOT_RETRYABLE
}
