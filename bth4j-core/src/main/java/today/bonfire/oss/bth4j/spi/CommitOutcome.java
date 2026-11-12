package today.bonfire.oss.bth4j.spi;

/** Result of {@link ScheduleStore#commitSchedule}. */
public enum CommitOutcome {
    /** the definition advanced and the occurrence is stored */
    STORED,
    /** the definition advanced; the occurrence is not stored, because another task holds its unique key */
    SKIPPED,
    /** nothing was written: the definition changed meanwhile, or a task with the occurrence's id exists */
    NOT_COMMITTED;

    /** @return true when the definition advanced */
    public boolean isCommitted() {
        return this != NOT_COMMITTED;
    }
}
