package today.bonfire.oss.bth4j.spi;

/** Number of tasks per state in one queue. */
public record StateCounts(
        long queued, long scheduled, long leased, long succeeded, long dead, long cancelled, long expired) {

    public static final StateCounts ZERO = new StateCounts(0, 0, 0, 0, 0, 0, 0);

    public long of(TaskState state) {
        return switch (state) {
            case QUEUED -> queued;
            case SCHEDULED -> scheduled;
            case LEASED -> leased;
            case SUCCEEDED -> succeeded;
            case DEAD -> dead;
            case CANCELLED -> cancelled;
            case EXPIRED -> expired;
        };
    }
}
