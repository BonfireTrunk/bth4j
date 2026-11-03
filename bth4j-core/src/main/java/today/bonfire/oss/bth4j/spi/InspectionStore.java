package today.bonfire.oss.bth4j.spi;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Read-only views for clients, the runner and the admin API. */
public interface InspectionStore extends TaskStore {

    /** @return the task with that id, in whatever state it is */
    Optional<TaskRecord> getTask(long id);

    /**
     * @return the first task id that {@link #allocateIds} has not handed out yet; every id below it was
     *         allocated by some client (it may never have been stored)
     */
    long idHighWaterMark();

    /** @return counts per state for every queue that has or had tasks, by queue id */
    Map<Integer, StateCounts> counts();

    /**
     * Lists tasks in one state, by {@code (stateChanged, id)} ascending, for keyset paging.
     *
     * @param queueId null for every queue
     * @param afterId only tasks after this id in the listing order; 0 starts at the beginning
     */
    List<TaskRecord> tasks(TaskState state, Integer queueId, long afterId, int limit);

    /**
     * Lists tasks in one state in the same order as {@link #tasks(TaskState, Integer, long, int)}, paging by an
     * opaque position that the store encodes itself. The position does not depend on the task it was taken from, so
     * that task leaving the listed state neither ends the listing early nor restarts it. A task whose own position
     * moves while the walk runs can still be seen twice (moved later) or missed (moved earlier).
     *
     * @param queueId null for every queue
     * @param cursor {@link TaskPage#next()} of the previous page; null starts at the beginning
     * @throws IllegalArgumentException if the cursor was not produced by this store
     */
    TaskPage tasks(TaskState state, Integer queueId, String cursor, int limit);
}
