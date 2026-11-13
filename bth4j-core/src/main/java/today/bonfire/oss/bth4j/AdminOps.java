package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * Operator view and operations over one store. {@link TaskClient#admin()} gives one without runner health and
 * issues; {@link BackgroundRunner#admin()} gives one that also reports the runner it belongs to.
 * <p>
 * Every change to a task takes the revision the operator saw. A refusal changes nothing and says why: no such
 * task, a revision that differs from what the operator saw, or a state that does not allow the change. The
 * store decides in that order in the same atomic step as the change.
 */
public interface AdminOps {
    Map<String, StateCounts> counts();

    List<QueueInfo> queues();

    /** @throws TaskValidationException when the queue is null, blank or unknown */
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
     * Lists the tasks of every queue in one state.
     *
     * @param cursor {@link TaskViewPage#next()} of the previous page; null starts at the beginning
     * @param limit the most tasks one page holds, at least 1. There is no upper bound, and a page is read into
     *     memory as a whole; the HTTP API caps it at 200.
     * @throws TaskValidationException if the state is null, the cursor is malformed or the limit is below 1
     */
    TaskViewPage tasks(TaskState state, String cursor, int limit);

    /**
     * Lists the tasks of one queue in one state.
     *
     * @param queue a known queue name; never null or blank
     * @param cursor {@link TaskViewPage#next()} of the previous page; null starts at the beginning
     * @param limit as for {@link #tasks(TaskState, String, int)}
     * @throws TaskValidationException when the state is null, the queue is null, blank or unknown, the cursor is
     *     malformed or the limit is below 1
     */
    TaskViewPage tasks(TaskState state, String queue, String cursor, int limit);

    /**
     * Requeues a DEAD, CANCELLED or EXPIRED task with attempts reset and revision incremented; failures and
     * steps are kept. A key still held inside its keep window is retained without expiry while the task is
     * open, and its keep window applies again when it finishes. A released key is not taken again: the retry
     * runs without a key, and an enqueue with that key is accepted as a second task when no other task holds it.
     *
     * @return {@link RetryOutcome#RETRIED} on success, else {@link RetryOutcome#NOT_FOUND},
     *         {@link RetryOutcome#STALE} (another revision) or {@link RetryOutcome#NOT_RETRYABLE} (another state)
     * @throws TaskStoreException when another task holds the task's unique key,
     *         which only damaged data can cause; nothing changes then
     */
    RetryOutcome retry(long id, int revision);

    /**
     * Cancels a QUEUED or SCHEDULED task at once and asks the runner of a LEASED one to stop.
     *
     * @return {@link CancelOutcome#CANCELLED} or {@link CancelOutcome#REQUESTED} on success, else
     *         {@link CancelOutcome#NOT_FOUND}, {@link CancelOutcome#STALE} (another revision) or
     *         {@link CancelOutcome#FINISHED} (the task is in a final state)
     */
    CancelOutcome cancel(long id, int revision, String reason);

    /**
     * Cancels the task that holds the unique key, with the revision read for it, as {@link #cancel(long, int, String)}
     * does by id; cancelling a waiting fixed-delay occurrence (key {@code sched:<schedule id>}) pauses its schedule.
     * A task retried inside its keep window is still reachable by key. A retry after key release is not;
     * this operation targets the current holder, which may be a different task.
     *
     * @return {@link CancelOutcome#NOT_FOUND} when no task holds the key, {@link CancelOutcome#FINISHED} when the
     *         holder has already finished (it only keeps the key for its window), otherwise the outcome of
     *         {@code cancel} for the holder. That is {@link CancelOutcome#STALE} when the holder changed between
     *         the read and the cancel, and can be {@link CancelOutcome#NOT_FOUND} or {@link CancelOutcome#FINISHED}
     *         when the holder was removed or finished in between.
     */
    CancelOutcome cancelByKey(String key, String reason);

    /**
     * Removes a task in any state but LEASED.
     *
     * @return {@link DeleteOutcome#DELETED} on success, else {@link DeleteOutcome#NOT_FOUND},
     *         {@link DeleteOutcome#STALE} (another revision) or {@link DeleteOutcome#LEASED}
     */
    DeleteOutcome delete(long id, int revision);

    /** @return the registered runners in the order of their ids, with the queues and types each processes */
    List<RunnerView> runners();

    /**
     * Uses a one-minute heartbeat window to find unserved waiting work. A runner writes its heartbeat every
     * {@code maintenanceCheckInterval} (5 seconds unless set); where runners use an interval above about 20
     * seconds, pass a window of at least three intervals to {@link #unservedWork(Duration)}, or live runners
     * count as absent between two heartbeats.
     */
    default List<UnservedWork> unservedWork() {
        return unservedWork(Duration.ofMinutes(1));
    }

    /**
     * Finds queued work on unpaused queues without a live accepting runner. Queue and type counts are exact.
     *
     * @param liveWithin maximum heartbeat age, measured against the admin clock; not negative and at most
     *     {@link TaskRequest#MAX_TIME_FROM_NOW}
     * @throws TaskValidationException for another {@code liveWithin}
     */
    List<UnservedWork> unservedWork(Duration liveWithin);

    /**
     * Deletes runner rows whose last heartbeat is older than {@code staleAfter}.
     *
     * @param staleAfter positive and at most {@link TaskRequest#MAX_TIME_FROM_NOW}
     * @return how many were deleted
     */
    int purgeStaleRunners(Duration staleAfter);

    /**
     * Lists the schedule definitions ordered by id (by the code points of the ids).
     *
     * @param cursor {@link ScheduleViewPage#next()} of the previous page, an opaque string; null starts at the
     *     beginning
     * @param limit the most definitions one page holds, at least 1. There is no upper bound, and a page is read
     *     into memory as a whole; the HTTP API caps it at 200.
     * @throws TaskValidationException if the limit is below 1
     */
    ScheduleViewPage schedules(String cursor, int limit);

    /**
     * Lists the definitions that will fire next: those that are not paused, soonest next fire time first and by
     * id among equal times. A paused definition does not fire and is left out.
     *
     * @param limit the most definitions returned, at least 1
     * @throws TaskValidationException if the limit is below 1
     */
    List<ScheduleView> upcomingSchedules(int limit);

    /**
     * Pauses or resumes a definition. Every schedule write takes the {@link ScheduleView#revision()} the operator
     * saw, and is refused as {@link ScheduleOutcome#STALE} when a write that is not the runner's replaced or
     * re-timed the definition meanwhile. A pause and a runner fire keep the revision.
     *
     * @return {@link ScheduleOutcome#APPLIED} on success, else {@link ScheduleOutcome#NOT_FOUND} or
     *         {@link ScheduleOutcome#STALE}
     */
    ScheduleOutcome pauseSchedule(String id, long revision, boolean paused);

    /**
     * Fires the definition at the next recurring check, whatever its next fire time was and however late that
     * check comes: the misfire policy does not apply to a triggered fire. A paused definition fires nothing
     * until it is resumed.
     *
     * @return {@link ScheduleOutcome#APPLIED} on success, else {@link ScheduleOutcome#NOT_FOUND} or
     *         {@link ScheduleOutcome#STALE}
     */
    ScheduleOutcome triggerSchedule(String id, long revision);

    /**
     * Removes a definition.
     *
     * @return {@link ScheduleOutcome#APPLIED} on success, else {@link ScheduleOutcome#NOT_FOUND} or
     *         {@link ScheduleOutcome#STALE}
     */
    ScheduleOutcome deleteSchedule(String id, long revision);

    /** @return the health of the runner this instance belongs to, or empty for a client-only instance */
    Optional<RunnerHealth> health();

    /** @return the issues the runner recorded, oldest first, or an empty list for a client-only instance */
    List<Issue> issues();

    Map<Integer, String> queueNames();

    Map<Integer, String> typeNames();
}
