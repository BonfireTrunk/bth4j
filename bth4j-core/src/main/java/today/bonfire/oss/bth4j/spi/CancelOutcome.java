package today.bonfire.oss.bth4j.spi;

/** Result of {@link AdminStore#cancel}. */
public enum CancelOutcome {
    /** the task was QUEUED or SCHEDULED and is CANCELLED now */
    CANCELLED,
    /** the task is LEASED; the request is stored and its worker will finish it */
    REQUESTED,
    /** the revision did not match, the task is already terminal, or it does not exist */
    STALE,
    /** only {@code AdminOps.cancelByKey}: no task holds the key */
    NOT_FOUND,
    /** only {@code AdminOps.cancelByKey}: the task holding the key has finished and is kept only for its key window */
    FINISHED
}
