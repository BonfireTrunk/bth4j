package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.OptionalInt;

/**
 * Executes the task runners picked up from the queues and bounds how many run at once.
 * <p>
 * The task processor reserves slots before it claims tasks from the store, so that a claimed task always has
 * a slot to run in: {@link #reserve(int, Duration)}, then one {@link #execute(Runnable)} per claimed task,
 * then {@link #release(int)} for the slots it did not use.
 */
public interface BackgroundExecutor {

    /**
     * Reserves between 1 and {@code max} slots, waiting up to {@code wait} for the first one.
     *
     * @return the number of slots reserved, 0 if none became free within the wait time
     */
    int reserve(int max, Duration wait) throws InterruptedException;

    /** Gives back reserved slots that were not used by {@link #execute(Runnable)}. */
    void release(int slots);

    /**
     * Runs the task in one of the reserved slots. The slot is freed when the task finishes.
     *
     * @throws java.util.concurrent.RejectedExecutionException if the task cannot be accepted, for example after
     *                                                         {@link #shutdown()}; the slot is freed in that case
     */
    void execute(Runnable task);

    /** A task that may free its slot before it ends, by running the {@code freeSlot} it is given. */
    @FunctionalInterface
    interface SlotTask {
        void run(Runnable freeSlot);
    }

    /**
     * Runs the task in one of the reserved slots, like {@link #execute(Runnable)}, but lets the task free its slot
     * before it ends: the runner frees it when the processor returns, before it writes the outcome to the store.
     * Running {@code freeSlot} again does nothing. An executor that frees slots early must still bound the tasks
     * that freed their slot and have not ended, so that a slow store holds back claims.
     *
     * <p>By default {@code freeSlot} does nothing and the slot is freed when the task ends.
     *
     * @throws java.util.concurrent.RejectedExecutionException as {@link #execute(Runnable)}
     */
    default void executeWithSlot(SlotTask task) {
        execute(() -> task.run(() -> {}));
    }

    /** Reports the total reservation and claim result of a successful claim attempt, including empty claims. */
    default void claimed(int reserved, int claimed) {}

    /** @return the current concurrency limit, when the executor exposes one */
    default OptionalInt limit() {
        return OptionalInt.empty();
    }

    /** @return the number of tasks running right now */
    int activeCount();

    /** Stops accepting new tasks. Tasks that are already running are allowed to finish. */
    void shutdown();

    /**
     * Waits for running tasks to finish after {@link #shutdown()} was called.
     *
     * @return true if all tasks finished, false if the timeout elapsed first
     */
    boolean awaitTermination(Duration timeout) throws InterruptedException;
}
