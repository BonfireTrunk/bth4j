package today.bonfire.oss.bth4j;

/** Whether a recurring occurrence may start while the previous one has not finished. */
public enum Overlap {
    ALLOW,
    /** the occurrence is skipped; implemented with the unique key {@code sched:<id>} held until the task ends */
    SKIP
}
