package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/**
 * Operator view and operations over one store. {@link TaskClient#admin()} gives one without runner health and
 * issues; {@link BackgroundRunner#admin()} gives one that also reports the runner it belongs to.
 * <p>
 * Every change to a task takes the revision the operator saw; a stale revision changes nothing and is
 * reported as false, {@link CancelOutcome#STALE} or {@link RetryOutcome#STALE}.
 */
public interface AdminOps {
    Map<String, StateCounts> counts();

    List<QueueInfo> queues();

    void pauseQueue(String queue, boolean paused);

    Optional<TaskView> task(long id);

    /**
     * An operator retry inside the keep window retains the key, so this lookup returns the retried task.
     * A retry after the key was released runs without it and cannot be found by that key.
     *
     * @return the task that holds the unique key now, open or finished inside its keep window; empty when none does
     */
    Optional<TaskView> taskByKey(String key);

    /**
     * @param queue null for all queues; results are ordered by state change time then id and start after {@code afterId}.
     *              Paging by {@code afterId} is best effort: a task that changes state between pages can end the
     *              listing early or repeat it. Use the cursor form for a walk that does not depend on the last task of the previous page.
     */
    List<TaskView> tasks(TaskState state, String queue, long afterId, int limit);

    /**
     * @param queue null for all queues
     * @param cursor {@link TaskViewPage#next()} of the previous page; null starts at the beginning
     * @throws IllegalArgumentException if the cursor is malformed
     */
    TaskViewPage tasks(TaskState state, String queue, String cursor, int limit);

    /**
     * Requeues a DEAD, CANCELLED or EXPIRED task with attempts reset and revision incremented; failures and
     * steps are kept. A key still held inside its keep window is retained without expiry while the task is
     * open, and its keep window applies again when it finishes. A released key is not taken again: the retry
     * runs without a key, and an enqueue with that key is accepted as a second task when no other task holds it.
     *
     * @return {@link RetryOutcome#RETRIED} on success, {@link RetryOutcome#STALE} for a missing task or a
     *         revision or state mismatch, or {@link RetryOutcome#DUPLICATE} when another task holds the key;
     *         either refusal changes nothing
     */
    RetryOutcome retry(long id, int revision);

    CancelOutcome cancel(long id, int revision, String reason);

    /**
     * Cancels the task that holds the unique key, with the revision read for it, as {@link #cancel(long, int, String)}
     * does by id; cancelling a waiting fixed-delay occurrence (key {@code sched:<schedule id>}) pauses its schedule.
     * A task retried inside its keep window is still reachable by key. A retry after key release is not;
     * this operation targets the current holder, which may be a different task.
     *
     * @return {@link CancelOutcome#NOT_FOUND} when no task holds the key, {@link CancelOutcome#FINISHED} when the
     *         holder has already finished (it only keeps the key for its window), {@link CancelOutcome#STALE} when it
     *         changed between the read and the cancel, otherwise the outcome of {@code cancel}
     */
    CancelOutcome cancelByKey(String key, String reason);

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
