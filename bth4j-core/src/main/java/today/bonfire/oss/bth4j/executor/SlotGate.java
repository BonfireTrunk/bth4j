package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The slots of an executor plus a bound on tasks in flight: a reserved slot also takes an in-flight permit, a task
 * may give its slot back before it ends, and gives the in-flight permit back when it ends. A task that freed its
 * slot is still writing its outcome to the store; the in-flight bound, twice the slot limit, stops claims from
 * running ahead of a slow store.
 */
final class SlotGate {

    private final AdjustableSemaphore slots;
    private final AdjustableSemaphore inFlight;

    SlotGate(int limit) {
        this.slots = new AdjustableSemaphore(limit);
        this.inFlight = new AdjustableSemaphore(inFlightFor(limit));
    }

    /** The in-flight bound for a slot limit: as many tasks writing their outcome as may run. */
    static int inFlightFor(int limit) {
        return (int) Math.min(Integer.MAX_VALUE, 2L * limit);
    }

    int reserve(int max, Duration wait) throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        if (!inFlight.tryAcquire(wait.toNanos(), TimeUnit.NANOSECONDS)) {
            return 0;
        }
        boolean slot = false;
        try {
            slot = slots.tryAcquire(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
        } finally {
            if (!slot) inFlight.release();
        }
        if (!slot) return 0;
        int reserved = 1;
        while (reserved < max && inFlight.tryAcquire()) {
            if (!slots.tryAcquire()) {
                inFlight.release();
                break;
            }
            reserved++;
        }
        return reserved;
    }

    void release(int count) {
        if (count > 0) {
            slots.release(count);
            inFlight.release(count);
        }
    }

    /** Runs the task on the calling thread in a reserved slot, and gives back what it still holds when it ends. */
    void run(BackgroundExecutor.SlotTask task) {
        var slot = new AtomicBoolean(true);
        Runnable freeSlot = () -> {
            if (slot.compareAndSet(true, false)) slots.release();
        };
        try {
            task.run(freeSlot);
        } finally {
            freeSlot.run();
            inFlight.release();
        }
    }

    /**
     * Changes the slot limit and the in-flight bound with it. Held permits stay held after a shrink: available
     * permits can be negative until tasks give them back.
     */
    void resize(int oldLimit, int newLimit) {
        if (newLimit > oldLimit) {
            slots.release(newLimit - oldLimit);
            inFlight.release(inFlightFor(newLimit) - inFlightFor(oldLimit));
        } else if (newLimit < oldLimit) {
            slots.reducePermits(oldLimit - newLimit);
            inFlight.reducePermits(inFlightFor(oldLimit) - inFlightFor(newLimit));
        }
    }

    /** Free slots, negative while a shrink is still held. */
    int availableSlots() {
        return slots.availablePermits();
    }

    /** Slots a reserve could take now: free slots, as far as the in-flight bound leaves room for them. */
    int usable() {
        return Math.min(slots.availablePermits(), inFlight.availablePermits());
    }

    /** Free in-flight permits; for tests. */
    int availableInFlight() {
        return inFlight.availablePermits();
    }

    private static final class AdjustableSemaphore extends Semaphore {
        AdjustableSemaphore(int permits) {
            super(permits);
        }

        @Override
        public void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }
}
