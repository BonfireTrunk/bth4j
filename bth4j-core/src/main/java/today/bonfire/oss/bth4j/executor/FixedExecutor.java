package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/** Runs every task on its own virtual thread, at most {@code limit} at once. */
public final class FixedExecutor implements BackgroundExecutor {

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final SlotGate gate;
    private final AtomicInteger active = new AtomicInteger();
    private final int limit;

    public FixedExecutor(int limit) {
        if (limit < 1) throw new TaskConfigurationException("limit must be at least 1");
        this.limit = limit;
        this.gate = new SlotGate(limit);
    }

    @Override
    public OptionalInt limit() {
        return OptionalInt.of(limit);
    }

    @Override
    public int reserve(int max, Duration wait) throws InterruptedException {
        return gate.reserve(max, wait);
    }

    @Override
    public void release(int count) {
        gate.release(count);
    }

    @Override
    public void execute(Runnable task) {
        executeWithSlot(freeSlot -> task.run());
    }

    /** The slot may be freed early; at most {@code 2 * limit} tasks run or write their outcome at once. */
    @Override
    public void executeWithSlot(SlotTask task) {
        try {
            executor.execute(() -> {
                active.incrementAndGet();
                try {
                    gate.run(task);
                } finally {
                    active.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            gate.release(1);
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
