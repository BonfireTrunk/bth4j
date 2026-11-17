package today.bonfire.oss.bth4j;

/** Result of {@link AdminStore#delete}. The store answers the first that applies, in the order listed. */
public enum DeleteOutcome {
    /** the task is removed */
    DELETED,
    /** no task has this id */
    NOT_FOUND,
    /** the revision did not match */
    STALE,
    /** the task is LEASED; a running task cannot be deleted */
    LEASED
}
