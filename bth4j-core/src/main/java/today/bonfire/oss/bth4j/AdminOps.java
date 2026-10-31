package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/**
 * Operator view and operations over one store. {@link TaskClient#admin()} gives one without runner health and
 * issues; {@link BackgroundRunner#admin()} gives one that also reports the runner it belongs to.
 * <p>
 * Every change to a task takes the revision the operator saw; a stale revision changes nothing and is
 * reported as false or {@link CancelOutcome#STALE}.
 */
public interface AdminOps {
    Map<String, StateCounts> counts();

    List<QueueInfo> queues();

    void pauseQueue(String queue, boolean paused);

    Optional<TaskView> task(long id);

    /** @param queue null for all queues; results are ordered by id and start after {@code afterId} */
    List<TaskView> tasks(TaskState state, String queue, long afterId, int limit);

    boolean retry(long id, int revision);

    CancelOutcome cancel(long id, int revision, String reason);

    boolean delete(long id, int revision);

    List<WorkerInfo> workers();

    int purgeStaleWorkers(Duration staleAfter);

    List<ScheduleView> schedules();

    boolean pauseSchedule(String id, boolean paused);

    /** fires the definition at the next check, whatever its next fire time was */
    boolean triggerSchedule(String id);

    boolean deleteSchedule(String id);

    /** @return the health of the runner this instance belongs to, or empty for a client-only instance */
    Optional<RunnerHealth> health();

    /** @return the issues the runner recorded, oldest first, or an empty list for a client-only instance */
    List<Issue> issues();

    Map<Integer, String> queueNames();

    Map<Integer, String> eventNames();
}
