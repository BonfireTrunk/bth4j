package today.bonfire.oss.bth4j.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Background housekeeping. Any runner may call these; the store makes them safe to run concurrently. */
public interface MaintenanceStore extends TaskStore {

    /** Moves SCHEDULED tasks whose due time has passed to QUEUED, keeping their due time, up to the limit. */
    PromoteResult promote(int limit);

    /** @return LEASED tasks whose lease expired, oldest first, up to the limit; nothing is changed */
    List<ExpiredLease> reclaim(int limit);

    /**
     * Removes tasks in the given terminal state whose {@link TaskRecord#stateChanged()} is older than
     * {@code retention}, oldest first, up to the limit. A task whose unique key is still held
     * ({@link TaskRecord#keyUntil()} in the future) is kept.
     *
     * @return how many were removed
     */
    long purge(TaskState state, Duration retention, int limit);

    /**
     * Releases unique keys of finished tasks whose {@link TaskRecord#keyUntil()} has passed.
     *
     * @return how many were released
     */
    int releaseKeys(int limit);

    /** @return the earliest due time among SCHEDULED tasks, or null */
    Instant nextDue();

    /**
     * Moves QUEUED and SCHEDULED tasks whose {@link TaskRecord#startBy()} has passed to EXPIRED (revision +1,
     * unique key handled like a terminal transition), oldest first, up to the limit.
     *
     * @return the ids moved
     */
    List<Long> expire(int limit);

    /**
     * Deletes worker rows whose last heartbeat is older than {@code staleAfter}.
     *
     * @return how many were deleted
     */
    int purgeWorkers(Duration staleAfter);
}
