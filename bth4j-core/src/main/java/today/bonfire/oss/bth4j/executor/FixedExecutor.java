package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Runs every task on its own virtual thread, at most {@code limit} at once. */
public final class FixedExecutor implements BackgroundExecutor {

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore slots;
    private final AtomicInteger active = new AtomicInteger();
    private final int limit;

    public FixedExecutor(int limit) {
        if (limit < 1) throw new IllegalArgumentException("limit must be at least 1");
        this.limit = limit;
        this.slots = new Semaphore(limit);
    }

    public int limit() {
        return limit;
    }

    @Override
    public int reserve(int max, Duration wait) throws InterruptedException {
        if (!slots.tryAcquire(wait.toNanos(), TimeUnit.NANOSECONDS)) {
            return 0;
        }
        int reserved = 1;
        while (reserved < max && slots.tryAcquire()) {
            reserved++;
        }
        return reserved;
    }

    @Override
    public void release(int count) {
        if (count > 0) slots.release(count);
    }

    @Override
    public void execute(Runnable task) {
        try {
            executor.execute(() -> {
                active.incrementAndGet();
                try {
                    task.run();
                } finally {
                    active.decrementAndGet();
                    slots.release();
                }
            });
        } catch (RuntimeException e) {
            slots.release();
            throw e;
        }
    }

    @Override
    public int activeCount() {
        return active.get();
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
