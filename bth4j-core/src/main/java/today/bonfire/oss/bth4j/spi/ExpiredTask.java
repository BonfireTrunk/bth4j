package today.bonfire.oss.bth4j.spi;

/**
 * A task that {@link MaintenanceStore#expire} moved to EXPIRED, with the queue and type it had. A store that
 * cannot read a number from a damaged record reports 0, which is never a registered id.
 */
public record ExpiredTask(long id, int queueId, int typeId) {}
