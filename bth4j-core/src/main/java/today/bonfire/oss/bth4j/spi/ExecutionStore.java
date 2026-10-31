package today.bonfire.oss.bth4j.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The transitions workers and producers make. Each method is atomic together with its checks: a
 * revision-checked transition has no effect, and reports so, when the task's revision or state differs from
 * what the caller expects. Only the worker holding the current revision of a LEASED task can move it on.
 * <p>
 * Revision rules: claim, promote, every terminal transition, reschedule, unclaim and admin changes to a
 * task that is not LEASED add one to the revision. {@link #renew}, {@link #saveStep}, {@link #progress} and
 * admin changes to a LEASED task do not, so a running worker keeps its fence.
 */
public interface ExecutionStore extends TaskStore {

    /**
     * Stores a new task in state QUEUED, or SCHEDULED when {@link TaskRecord#due()} is in the future. The
     * store sets {@code due} to its own time when it is null, and sets {@code stateChanged}.
     * <p>
     * When the record has a {@link TaskRecord#uniqueKey()} and another task holds it, nothing is stored and
     * {@link EnqueueOutcome.DuplicateKey} names that task. A key is held while its task is not terminal, and
     * after that until {@link TaskRecord#keyUntil()} when {@link TaskRecord#keyKeep()} was set.
     *
     * @return {@link EnqueueOutcome#STORED}, {@link EnqueueOutcome#EXISTS} when the id already exists in
     *         any state (the caller's earlier write succeeded), or a duplicate-key outcome
     */
    EnqueueOutcome enqueue(TaskRecord task);

    /** {@link #enqueue} for several tasks, in order, in as few round trips as the store allows. */
    List<EnqueueOutcome> enqueueAll(List<TaskRecord> tasks);

    /**
     * Moves up to {@code count} queued tasks to LEASED and returns them with their new revision, lease and
     * worker. Queues are read in the given order by {@code (due, id)}. Tasks whose event is not in
     * {@link ClaimRequest#knownEventIds()} are left alone; a store that cannot filter cheaply may instead
     * move their due time a few seconds ahead without counting an attempt. Each claim adds one to
     * {@code attempts}, sets {@code claimedAt} and keeps {@code due}.
     */
    List<TaskRecord> claim(ClaimRequest request);

    /**
     * Extends the lease of each task whose revision still matches and is LEASED.
     *
     * @return one status per lease, in the same order
     */
    List<LeaseStatus> renew(List<LeaseRef> leases, Duration lease);

    /**
     * Completes a LEASED task: state SUCCEEDED, or the record is removed when {@code keep} is false. The
     * unique key is released or scheduled for release per {@link TaskRecord#keyKeep()}.
     *
     * @param keepPayload false drops {@link TaskRecord#data()}; steps are kept
     * @return false when the revision did not match
     */
    boolean ack(long id, int revision, boolean keep, boolean keepPayload);

    /**
     * Records a failed attempt of a LEASED task. The task is SCHEDULED for {@code retryAt}, or DEAD when
     * {@code retryAt} is null or the attempts are exhausted. The failure is appended to
     * {@link TaskRecord#failures()}, which the store bounds to the last 20.
     *
     * @param keepDead false removes a dead task at once instead of keeping it for inspection
     */
    FailOutcome fail(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead);

    /**
     * Same as {@link #fail} for a task whose lease {@link MaintenanceStore#reclaim} found expired; also
     * {@link FailOutcome#LOST} when the lease is no longer expired, because its worker renewed it meanwhile.
     */
    FailOutcome failExpired(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead);

    /**
     * Moves a LEASED task to SCHEDULED for {@code due} and gives the attempt back.
     *
     * @return false when the revision did not match
     */
    boolean reschedule(long id, int revision, Instant due);

    /**
     * Returns a LEASED task to its queue with its due time unchanged and gives the attempt back.
     *
     * @return false when the revision did not match
     */
    boolean unclaim(long id, int revision);

    /**
     * Replaces the steps of a LEASED task. The caller passes the complete map; the store writes it as given.
     *
     * @return false when the revision did not match
     */
    boolean saveSteps(long id, int revision, java.util.Map<String, StepResult> steps);

    /**
     * Sets the progress text of a LEASED task.
     *
     * @return false when the revision did not match
     */
    boolean progress(long id, int revision, String progress);
}
