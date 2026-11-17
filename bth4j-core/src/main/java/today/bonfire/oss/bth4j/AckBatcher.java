package today.bonfire.oss.bth4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.spi.ExecutionStore;
import today.bonfire.oss.bth4j.spi.LeaseRef;

/**
 * Sends the acknowledgements of tasks that finish while an earlier batch is on its way to the store as one
 * {@link ExecutionStore#ackAll} call. It has no thread and no timer: a finishing thread that finds no batch on its
 * way sends everything waiting, its own acknowledgement included, so a quiet runner acknowledges at once. One that
 * finds a batch on its way waits; when that batch returns, the first thread still waiting sends the next. Each
 * thread sends at most one batch, so a task waits for at most the batch before its own. A returning batch wakes
 * only its own threads and the next sender; the threads waiting for that next batch stay asleep.
 */
final class AckBatcher {

    private final ExecutionStore store;
    private final boolean keep;
    private final boolean keepPayload;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<Pending> waiting = new ArrayList<>();
    private boolean sending;

    AckBatcher(ExecutionStore store, boolean keep, boolean keepPayload) {
        this.store = store;
        this.keep = keep;
        this.keepPayload = keepPayload;
    }

    private static final class Pending {
        final LeaseRef lease;
        /** Signalled when this acknowledgement is done or is to send the next batch. */
        final Condition woken;

        boolean send;
        boolean done;
        boolean applied;
        Throwable failure;

        Pending(LeaseRef lease, Condition woken) {
            this.lease = lease;
            this.woken = woken;
        }
    }

    /**
     * {@link ExecutionStore#ack} without a schedule effect, with the runner's keep settings.
     *
     * @return false when the revision did not match
     * @throws RuntimeException when the batch call failed; the outcome is unknown then
     */
    boolean ack(long id, int revision) {
        var mine = new Pending(new LeaseRef(id, revision), lock.newCondition());
        List<Pending> batch;
        lock.lock();
        try {
            waiting.add(mine);
            if (!sending) {
                sending = true;
                mine.send = true;
            }
            while (!mine.done && !mine.send) mine.woken.awaitUninterruptibly();
            if (mine.done) return result(mine);
            batch = new ArrayList<>(waiting);
            waiting.clear();
        } finally {
            lock.unlock();
        }
        List<Boolean> applied = null;
        Throwable failure = null;
        try {
            applied = store.ackAll(batch.stream().map(p -> p.lease).toList(), keep, keepPayload);
            if (applied.size() != batch.size()) {
                failure = new TaskStoreException(
                        "store answered " + applied.size() + " acknowledgements for " + batch.size());
            }
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        lock.lock();
        try {
            for (var i = 0; i < batch.size(); i++) {
                var p = batch.get(i);
                p.done = true;
                if (failure != null) p.failure = failure;
                else p.applied = applied.get(i);
                if (p != mine) p.woken.signal();
            }
            if (waiting.isEmpty()) {
                sending = false;
            } else {
                var next = waiting.getFirst();
                next.send = true;
                next.woken.signal();
            }
        } finally {
            lock.unlock();
        }
        if (failure instanceof Error e) throw e;
        return result(mine);
    }

    /** Acknowledgements waiting for a batch; for tests. */
    int waitingCount() {
        lock.lock();
        try {
            return waiting.size();
        } finally {
            lock.unlock();
        }
    }

    /** A thread that only waited gets an Error of the sender wrapped, so that only the sender rethrows it as fatal. */
    private static boolean result(Pending p) {
        return switch (p.failure) {
            case null -> p.applied;
            case RuntimeException e -> throw e;
            default -> throw new TaskStoreException("acknowledgement batch failed: " + p.failure, p.failure);
        };
    }
}
