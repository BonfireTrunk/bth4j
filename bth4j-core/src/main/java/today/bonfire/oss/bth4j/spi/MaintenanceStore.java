package today.bonfire.oss.bth4j.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Background housekeeping. Any runner may call these; the store makes them safe to run concurrently. A call
 * with a limit fills the batch while more qualify (an index entry the store finds broken and drops may take a
 * place in it): the runner calls again while a batch comes back full, and a short batch ends the round. A
 * limit is at least 1; a lower one throws {@link IllegalArgumentException}.
 */
public interface MaintenanceStore extends TaskStore {

    /** Moves SCHEDULED tasks whose due time has passed to QUEUED, keeping their due time, up to the limit. */
    PromoteResult promote(int limit);

    /** @return LEASED tasks whose lease expired, oldest first, up to the limit; nothing is changed */
    List<ExpiredLease> reclaim(int limit);

    /**
     * Removes tasks in the given terminal state whose {@link TaskRecord#stateChangedAt()} is older than
     * {@code retention}, up to the limit, oldest first within each queue (a store may take one queue after
     * another). A removed task whose unique key is still held ({@link TaskRecord#keyUntil()} in the future)
     * leaves the key taken until then: {@link #releaseKeys} frees it, and a same-key enqueue meanwhile answers
     * {@link EnqueueOutcome.DuplicateKey} with the removed task's id.
     *
     * @param state SUCCEEDED, DEAD, CANCELLED or EXPIRED; any other state throws {@link IllegalArgumentException}
     * @return how many were removed
     */
    long purge(TaskState state, Duration retention, int limit);

    /**
     * Releases unique keys of finished tasks whose {@link TaskRecord#keyUntil()} has passed, including the keys
     * that removed tasks left taken.
     *
     * @param limit the most keys one call releases
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
