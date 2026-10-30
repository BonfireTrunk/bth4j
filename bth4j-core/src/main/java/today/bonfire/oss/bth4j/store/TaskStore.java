package today.bonfire.oss.bth4j.store;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Persistent state of tasks and recurring definitions, and the transitions between their states.
 * <p>
 * A task is in exactly one of four states: <em>queued</em> (waiting in its queue, taken in FIFO order),
 * <em>scheduled</em> (waiting for its due time), <em>leased</em> (claimed by a worker that holds the task's
 * lease token) or <em>dead</em> (attempts exhausted, kept for inspection). Recurring definitions are kept
 * apart from tasks; each occurrence of a definition is a task of its own.
 * <p>
 * An implementation must guarantee that
 * <ul>
 *   <li>{@link #claim} hands each queued task to exactly one caller and gives it a token nobody else holds;</li>
 *   <li>every token-checked transition ({@link #renew}, {@link #ack}, {@link #fail}, {@link #reschedule},
 *       {@link #unclaim}) is atomic together with its check and has no effect when the token does not
 *       match the one stored with the task;</li>
 *   <li>{@link #enqueue} and {@link #commitRecurring} never overwrite a task whose id already exists;</li>
 *   <li>{@link #commitRecurring} succeeds at most once per schedule revision and fire time, across all
 *       runners that share the store.</li>
 * </ul>
 * All methods are called concurrently, from several threads of one runner and from several runners
 * sharing the store. They throw an unchecked exception when the store cannot be reached; callers retry
 * on their next cycle. Times are instants, the store decides how to represent them.
 */
public interface TaskStore {

    /**
     * Stores a new task. It is queued at once, or scheduled when {@link TaskRecord#due()} is set.
     *
     * @return true when stored, false when a task with that id already exists in any state
     */
    boolean enqueue(TaskRecord task);

    /**
     * Creates or replaces a recurring definition. The definition's revision is assigned by the store and
     * the value passed in is ignored. When a definition with that id exists and its cron expression is
     * unchanged, the stored next fire time is kept; otherwise it becomes {@code nextFire}.
     */
    void putRecurring(RecurringDefinition definition, Instant nextFire);

    /** @return true when a definition with that id existed */
    boolean deleteRecurring(String scheduleId);

    /** @return every recurring definition's id with its next fire time */
    Map<String, Instant> recurringSchedules();

    /** @return the task with that id, in whatever state it is */
    Optional<TaskRecord> getTask(String id);

    long queueSize(String queue);

    long leasedCount();

    long scheduledCount();

    long deadCount();

    /** @return the oldest dead tasks, up to the limit */
    List<TaskRecord> deadTasks(int limit);

    /**
     * Puts a dead task back into its queue with its attempts reset to zero.
     *
     * @return true when the task was dead
     */
    boolean retryDeadTask(String id);

    /** Removes dead tasks that died before the given time. @return the number removed */
    long purgeDeadTasks(Instant diedBefore);

    /**
     * Moves up to {@code count} queued tasks to the leased state and returns them with their new token.
     * The queues are drained in the given order: the second queue is only read when the first one could
     * not fill the count. Each claim increments the task's attempts.
     *
     * @param tokenBase unique per call; the store derives one token per task from it
     * @param lease     how long the lease lasts unless renewed
     */
    List<TaskRecord> claim(List<String> queues, int count, String tokenBase, Duration lease);

    /**
     * Extends the lease of each task whose stored token still equals the given one.
     *
     * @param tokensById task id to the token its worker holds
     * @return how many leases were extended
     */
    int renew(Map<String, String> tokensById, Duration lease);

    /** Removes a completed task. @return false when the token did not match */
    boolean ack(String id, String token);

    /**
     * Records a failed attempt. The task is scheduled again after {@code retryDelay}, or moved to the dead
     * state when its attempts are exhausted or {@code retryDelay} is null.
     *
     * @param error       what went wrong, at most 1000 characters, kept with the task
     * @param keepDeadFor how long a dead task must stay readable; the store may drop it afterwards even
     *                    before {@link #purgeDeadTasks} runs
     */
    FailOutcome fail(String id, String token, Duration retryDelay, String error, Duration keepDeadFor);

    /**
     * Moves a leased task to the scheduled state for the given time without counting the attempt.
     *
     * @return false when the token did not match
     */
    boolean reschedule(String id, String token, Instant due);

    /**
     * Returns a leased task to the front of its queue without counting the attempt.
     *
     * @return false when the token did not match
     */
    boolean unclaim(String id, String token);

    /** Moves scheduled tasks whose due time has passed into their queues, up to the limit. */
    PromoteResult promote(int limit);

    /** @return leased tasks whose lease expired, oldest first, up to the limit; nothing is changed */
    List<ExpiredLease> reclaim(int limit);

    /** @return recurring definitions whose next fire time is at or before the given time, up to the limit */
    List<DueSchedule> dueRecurring(Instant upTo, int limit);

    /**
     * Creates the occurrence task (scheduled for {@link DueSchedule#fireAt()}) and advances the definition
     * to {@code nextFire}, unless the definition's revision or next fire time changed meanwhile.
     *
     * @return true when committed
     */
    boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord occurrence);
}
