package today.bonfire.oss.bth4j.spi;

/**
 * A leased task whose lease expired, as returned by {@link MaintenanceStore#reclaim}. {@code queueId} and
 * {@code typeId} come from the same read as the lease; a store that cannot read one reports 0, which is never a
 * registered id.
 */
public record ExpiredLease(
        long id, int revision, int attempts, int maxAttempts, int queueId, int typeId, boolean cancelRequested) {}
