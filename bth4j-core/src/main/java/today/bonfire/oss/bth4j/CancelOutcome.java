package today.bonfire.oss.bth4j;

/** Result of {@link AdminStore#cancel}. The store answers the first that applies, in the order listed. */
public enum CancelOutcome {
    /** the task was QUEUED or SCHEDULED and is CANCELLED now */
    CANCELLED,
    /** the task is LEASED; the request is stored and its worker will finish it */
    REQUESTED,
    /** no task has this id, or (only {@code AdminOps.cancelByKey}) no task holds the key */
    NOT_FOUND,
    /** the revision did not match */
    STALE,
    /** the task is already in a final state; for {@code AdminOps.cancelByKey}, the holder has finished and is kept only for its key window */
    FINISHED
}
