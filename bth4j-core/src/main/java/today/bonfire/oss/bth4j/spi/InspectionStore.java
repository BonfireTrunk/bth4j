package today.bonfire.oss.bth4j.spi;

import java.util.Map;
import java.util.Optional;

/** Read-only views for clients, the runner and the admin API. */
public interface InspectionStore extends TaskStore {

    /**
     * @return the task with that id, in whatever state it is
     * @throws UnreadableTaskException when a stored value of the task cannot be read
     */
    Optional<TaskRecord> getTask(long id);

    /**
     * @return the first task id that {@link #allocateIds} has not handed out yet; every id below it was
     *         allocated by some client (it may never have been stored)
     */
    long idHighWaterMark();

    /**
     * @return the task that holds the unique key now: an open task (QUEUED, SCHEDULED, LEASED) or a finished one
     *         whose keep window has not passed; empty when no task holds the key, and also when the holder was
     *         removed inside its keep window and only its key stays taken (see {@link MaintenanceStore#purge})
     */
    Optional<TaskRecord> taskByKey(String key);

    /**
     * @return true when the task with this id holds the unique key now: as an open task, as a finished one whose
     *         keep window has not passed, or as one that was removed inside its keep window, which
     *         {@link #taskByKey} does not show
     */
    boolean holdsKey(long id, String key);

    /**
     * @return counts per state by queue id, for every registered queue; a queue without tasks has every count
     *         zero
     */
    Map<Integer, StateCounts> counts();

    /** @return exact QUEUED counts by queue id and type id, with only non-zero entries */
    Map<Integer, Map<Integer, Long>> queuedByType();

    /**
     * Lists tasks in one state, QUEUED and SCHEDULED by {@code (due, id)} ascending and the other states by
     * {@code (stateChanged, id)} ascending, paging by an opaque position that the store encodes itself. Among
     * tasks that share the time, a store may order the ids its own way, as long as the order is total and stable
     * (the Redis store compares them as text). The
     * position does not depend on the task it was taken from, so that task leaving the listed state neither ends
     * the listing early nor restarts it. A task whose own position moves while the walk runs can still be seen
     * twice (moved later) or missed (moved earlier). A task with a stored value that cannot be read is left out,
     * so that it does not break the listing; it still takes its place in the walk, so a page can hold fewer than
     * {@code limit} tasks while {@code next} is not null, and it is still counted by {@link #counts()}.
     *
     * @param queueId null for every queue
     * @param cursor {@link TaskPage#next()} of the previous page; null starts at the beginning
     * @param limit the most tasks one page holds, at least 1
     * @throws IllegalArgumentException if the cursor was not produced by this store or the limit is below 1
     */
    TaskPage tasks(TaskState state, Integer queueId, String cursor, int limit);
}
