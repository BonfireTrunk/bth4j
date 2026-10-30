package today.bonfire.oss.bth4j.store;

/** Result of {@link TaskStore#fail}. */
public enum FailOutcome {
    /** scheduled again after the retry delay */
    RETRY,
    /** attempts exhausted, or no retry was allowed */
    DEAD,
    /** the lease token no longer matched, another runner owns the task now */
    LOST
}
