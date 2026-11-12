package today.bonfire.oss.bth4j;

/** What {@link TaskClient#putSchedule} did. */
public enum PutScheduleOutcome {
    /** no definition with that id existed */
    CREATED,
    /** the definition, its next fire time or its recorded occurrence changed; it has a new revision */
    CHANGED,
    /** nothing was written and the revision is kept */
    UNCHANGED
}
