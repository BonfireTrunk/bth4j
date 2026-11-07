package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The slots of an executor plus a bound on tasks in flight: a reserved slot also takes an in-flight permit, a task
 * may give its slot back before it ends, and gives the in-flight permit back when it ends. A task that freed its
 * slot is still writing its outcome to the store; the in-flight bound stops claims from running ahead of a slow
 * store.
 */
final class SlotGate {

    private final Semaphore slots;
    private final Semaphore inFlight;

    /** @param inFlight how many tasks may hold a slot or be writing their outcome at once */
    SlotGate(Semaphore slots, int inFlight) {
        this.slots = slots;
        this.inFlight = new Semaphore(inFlight);
    }

    /** The in-flight bound for a slot limit: as many tasks writing their outcome as may run. */
    static int inFlightFor(int maxSlots) {
        return (int) Math.min(Integer.MAX_VALUE, 2L * maxSlots);
    }

    int reserve(int max, Duration wait) throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        if (!inFlight.tryAcquire(wait.toNanos(), TimeUnit.NANOSECONDS)) {
            return 0;
        }
        if (!slots.tryAcquire(deadline - System.nanoTime(), TimeUnit.NANOSECONDS)) {
            inFlight.release();
            return 0;
        }
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
}
