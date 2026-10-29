package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Executes the task runners picked up from the queues. Implementations decide how many tasks
 * run concurrently and tell the task processor when to stop picking up new tasks.
 */
public interface BackgroundExecutor {

    /**
     * @return the underlying executor service
     */
    ExecutorService getExecutor();

    /**
     * @return true if no more tasks should be submitted right now. The task processor then waits
     *     before taking the next task from Redis.
     */
    boolean isPoolFull();

    /**
     * Submits a task for execution.
     *
     * @param task the task to run
     * @throws java.util.concurrent.RejectedExecutionException if the task cannot be accepted
     */
    void execute(Runnable task);

    /**
     * Stops accepting new tasks. Tasks that are already running are allowed to finish.
     */
    void shutdown();

    /**
     * Waits for running tasks to finish after {@link #shutdown()} was called.
     *
     * @param timeout maximum time to wait
     * @return true if all tasks finished, false if the timeout elapsed first
     *
     * @throws InterruptedException if interrupted while waiting
     */
    default boolean awaitTermination(Duration timeout) throws InterruptedException {
        return getExecutor().awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
