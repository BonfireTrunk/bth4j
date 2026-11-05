package today.bonfire.oss.bth4j.spi;

/** Result of {@link ScheduleStore#putRecurring}. */
public enum PutRecurringOutcome {
    /** no definition with that id existed */
    CREATED,
    /** the definition, its next fire time or its recorded occurrence changed; it has a new revision */
    CHANGED,
    /** nothing was written and the revision is kept */
    UNCHANGED
}
