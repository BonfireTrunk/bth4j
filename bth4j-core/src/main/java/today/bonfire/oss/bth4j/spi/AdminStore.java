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
     */
    CancelOutcome cancel(long id, int revision, String reason);

    /**
     * Returns a DEAD, CANCELLED or EXPIRED task to its queue: QUEUED, due now, attempts 0, revision +1;
     * cancel fields, worker, lease, {@code keyKeep}, {@code keyUntil} and {@code uniqueKey} cleared (the key
     * was released at the terminal transition and may belong to another task now); failures and steps kept.
     *
     * @return false when the revision or state did not match
     */
    boolean retry(long id, int revision);

    /**
     * Removes a task in any state but LEASED and releases its unique key.
     *
     * @return false when the revision did not match or the task is LEASED
     */
    boolean delete(long id, int revision);

    /** Paused queues are skipped by {@link ExecutionStore#claim} on every runner. */
    void pauseQueue(int queueId, boolean paused);

    /** @return every queue the store knows, in id order */
    List<QueueInfo> queues();

    /** Records that the worker is alive now and how many tasks it is running. */
    void heartbeat(int workerId, int running);

    /** @return every worker row, in id order */
    List<WorkerInfo> workers();
}
