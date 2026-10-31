package today.bonfire.oss.bth4j.spi;

/** A leased task whose lease expired, as returned by {@link MaintenanceStore#reclaim}. */
public record ExpiredLease(
        long id, int revision, int attempts, int maxAttempts, int eventId, boolean cancelRequested) {}
