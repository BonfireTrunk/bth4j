package today.bonfire.oss.bth4j.spi;

/**
 * Result of {@link ExecutionStore#renew} for one lease.
 *
 * @param owned           false when the revision no longer matched: the task moved on without this worker
 * @param cancelRequested true when a cancel was requested for the task; the worker should stop
 */
public record LeaseStatus(long id, boolean owned, boolean cancelRequested) {}
