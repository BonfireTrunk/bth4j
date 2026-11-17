package today.bonfire.oss.bth4j;

/**
 * Result of an operator write on a schedule: delete, pause, resume or trigger. The store answers the first that
 * applies, in the order {@link #NOT_FOUND}, {@link #STALE}, {@link #APPLIED}.
 */
public enum ScheduleOutcome {
    /** the revision matched, or none was given; the write was made, or there was nothing to change */
    APPLIED,
    /** no schedule has this id */
    NOT_FOUND,
    /** the revision did not match */
    STALE
}
