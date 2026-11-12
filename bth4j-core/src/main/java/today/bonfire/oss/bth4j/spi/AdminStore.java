package today.bonfire.oss.bth4j.spi;

import java.util.List;
import java.util.Set;
import today.bonfire.oss.bth4j.CancelOutcome;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.QueueInfo;
import today.bonfire.oss.bth4j.RetryOutcome;

/**
 * Operator changes. {@code cancel}, {@code retry} and {@code delete} decide in one atomic step and answer, in
 * this order: no such task, a revision that differs from what the operator saw, then a state that does not
 * allow the change. A refusal has no effect.
 */
public interface AdminStore extends TaskStore {

    /**
     * Cancels a task. QUEUED or SCHEDULED: CANCELLED at once (revision +1, {@code cancelRequestedAt} and
     * {@code cancelReason} set, unique key handled like a terminal transition). LEASED: only the request is
     * stored ({@code cancelRequestedAt}, {@code cancelReason}, no revision change); the worker learns of it
     * through {@link ExecutionStore#renew} and finishes the task with {@link ExecutionStore#finishCancelled}.
     * The request stays with the task until it ends or {@link #retry} clears it: when the attempt fails with a
     * retry, reschedules or is unclaimed, the next attempt's claim and renew report it. The reason is stored as
     * given; null removes an earlier one, here and in {@code finishCancelled}.
     *
     * @return {@link CancelOutcome#NOT_FOUND} for a missing task, {@link CancelOutcome#STALE} for another
     *     revision, {@link CancelOutcome#FINISHED} for a task in a final state, else {@link CancelOutcome#CANCELLED}
     *     or {@link CancelOutcome#REQUESTED}
     * @param effect applied to a recurring definition in the same atomic step, only when the task is
     *     cancelled at once; may be null
     */
    CancelOutcome cancel(long id, int revision, String reason, ScheduleEffect effect);

    /**
     * Returns a DEAD, CANCELLED or EXPIRED task to its queue: QUEUED, due now, attempts 0, revision +1;
     * cancel fields, worker, lease and {@code startBy} cleared; failures and steps kept. A task that still
     * holds its unique key holds it again as an open task: {@code keyUntil} is cleared and {@code keyKeep}
     * is kept, so the keep window applies again when it finishes. If another task holds that key, which only
     * damaged data can cause, the store throws {@link TaskStoreException} and nothing changes.
     * A key released at the terminal transition or after its keep window is not taken again: the task retries
     * without a key, and an enqueue with that key is accepted as a second task when no other task holds it.
     *
     * @return {@link RetryOutcome#NOT_FOUND} for a missing task, {@link RetryOutcome#STALE} for another revision,
     *     {@link RetryOutcome#NOT_RETRYABLE} for a task in another state, else {@link RetryOutcome#RETRIED}
     */
    RetryOutcome retry(long id, int revision);

    /**
     * Removes a task in any state but LEASED and releases its unique key.
     *
     * @param effect applied to a recurring definition in the same atomic step when the task is removed; may
     *     be null
     * @return {@link DeleteOutcome#NOT_FOUND} for a missing task, {@link DeleteOutcome#STALE} for another
     *     revision, {@link DeleteOutcome#LEASED} for a running task, else {@link DeleteOutcome#DELETED}
     */
    DeleteOutcome delete(long id, int revision, ScheduleEffect effect);

    /**
     * Paused queues are skipped by {@link ExecutionStore#claim} on every runner. An id that was never registered
     * is ignored: a queue registered later under that id starts unpaused.
     */
    void pauseQueue(int queueId, boolean paused);

    /** @return every queue the store knows, in id order */
    List<QueueInfo> queues();

    /**
     * Records that the worker is alive now, how many tasks it is running and whether it accepts new work. When
     * the worker's row is gone, because {@link MaintenanceStore#purgeWorkers} removed it while the worker could
     * not reach the store, the row is created again under the same id with the label, queues and types given
     * here and a start time of now. An existing row keeps its own label, queues, types and start time. The
     * label is not null.
     */
    void heartbeat(
            int workerId, String label, Set<Integer> queueIds, Set<Integer> typeIds, int running, boolean accepting);

    /** @return every worker row, in id order */
    List<WorkerInfo> workers();
}
