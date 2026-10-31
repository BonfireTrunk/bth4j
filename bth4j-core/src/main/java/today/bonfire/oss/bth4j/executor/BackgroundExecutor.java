package today.bonfire.oss.bth4j.executor;

import java.time.Duration;

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
