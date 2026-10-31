package today.bonfire.oss.bth4j.spi;

/** Result of {@link ExecutionStore#fail} and {@link ExecutionStore#failExpired}. */
public enum FailOutcome {
    /** scheduled again for the retry time */
    RETRY,
    /** attempts exhausted, or no retry was allowed */
    DEAD,
    /** the revision no longer matched: the task moved on without this worker */
    LOST
}
