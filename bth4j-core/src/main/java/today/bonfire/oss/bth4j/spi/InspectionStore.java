package today.bonfire.oss.bth4j.spi;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Read-only views for clients, the runner and the admin API. */
public interface InspectionStore extends TaskStore {

    /** @return the task with that id, in whatever state it is */
    Optional<TaskRecord> getTask(long id);

    /** @return counts per state for every queue that has or had tasks, by queue id */
    Map<Integer, StateCounts> counts();

    /**
     * Lists tasks in one state, by {@code (stateChanged, id)} ascending, for keyset paging.
     *
     * @param queueId null for every queue
     * @param afterId only tasks after this id in the listing order; 0 starts at the beginning
     */
    List<TaskRecord> tasks(TaskState state, Integer queueId, long afterId, int limit);
}
