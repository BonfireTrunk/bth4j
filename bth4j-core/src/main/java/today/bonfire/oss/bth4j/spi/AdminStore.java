package today.bonfire.oss.bth4j.spi;

import java.util.List;

/**
 * Operator changes. Every revision-checked method has no effect and reports so when the task's revision or
 * state differs from what the operator saw.
 */
public interface AdminStore extends TaskStore {

    /**
     * Cancels a task. QUEUED or SCHEDULED: CANCELLED at once (revision +1, {@code cancelRequestedAt} and
     * {@code cancelReason} set, unique key handled like a terminal transition). LEASED: only the request is
     * stored ({@code cancelRequestedAt}, {@code cancelReason}, no revision change); the worker learns of it
     * through {@link ExecutionStore#renew} and finishes the task with {@link ExecutionStore#finishCancelled}.
     *
     * @param effect applied to a recurring definition in the same atomic step, only when the task is
     *     cancelled at once; may be null
     */
    CancelOutcome cancel(long id, int revision, String reason, ScheduleEffect effect);

    /**
     * Returns a DEAD, CANCELLED or EXPIRED task to its queue: QUEUED, due now, attempts 0, revision +1;
     * cancel fields, worker, lease and {@code startBy} cleared; failures and steps kept. A task that still
     * holds its unique key holds it again as an open task: {@code keyUntil} is cleared and {@code keyKeep}
     * is kept, so the keep window applies again when it finishes. If another task holds that key, nothing changes.
     * A key released at the terminal transition or after its keep window is not taken again: the task retries
     * without a key, and an enqueue with that key is accepted as a second task when no other task holds it.
     *
     * @return {@link RetryOutcome#STALE} when the task is missing or the revision or state did not match, {@link RetryOutcome#DUPLICATE}
     *     when the unique key is held by another task, else {@link RetryOutcome#RETRIED}
     */
    RetryOutcome retry(long id, int revision);

    /**
     * Removes a task in any state but LEASED and releases its unique key.
     *
     * @param effect applied to a recurring definition in the same atomic step when the task is removed; may
     *     be null
     * @return false when the revision did not match or the task is LEASED
     */
    boolean delete(long id, int revision, ScheduleEffect effect);

    /** Paused queues are skipped by {@link ExecutionStore#claim} on every runner. */
    void pauseQueue(int queueId, boolean paused);

    /** @return every queue the store knows, in id order */
    List<QueueInfo> queues();

    /** Records that the worker is alive now and how many tasks it is running. */
    void heartbeat(int workerId, int running);

    /** @return every worker row, in id order */
    List<WorkerInfo> workers();
}
