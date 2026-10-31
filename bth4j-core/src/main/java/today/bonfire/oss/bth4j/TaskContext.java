package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Callable;
import today.bonfire.oss.bth4j.exceptions.TaskCancelledException;

/** What a {@link TaskProcessor} knows about the task it is running and can report back while running. */
public interface TaskContext {

    /** @return the task as it was claimed */
    TaskView task();

    long id();

    /** @return the current attempt, starting at 1 */
    int attempt();

    Map<String, String> headers();

    /** @return the runner's clock */
    Clock clock();

    /** @return true when an operator asked for this task to stop */
    boolean cancelRequested();

    /** @throws TaskCancelledException when {@link #cancelRequested()} is true, which ends the task as cancelled */
    void checkCancelled();

    /**
     * Records free-form progress for operators. Stored on the next call, not buffered; keep it small and call
     * it at meaningful points.
     *
     * @param progress at most 4 KiB
     */
    void progress(String progress);

    /**
     * Runs {@code body} once per task, not once per attempt: when a step with this name already completed in
     * an earlier attempt, the stored result is returned and the body does not run. The result is stored before
     * this method returns, so a later attempt resumes after the last completed step. A step is repeated when
     * the worker dies between finishing the body and storing the result, so the body should be safe to repeat.
     *
     * @param name   unique within the task, at most 64 characters
     * @param type   result class, written and read with the runner's JSON mapper
     * @param body   the work
     * @return the result of this or an earlier run
     */
    <R> R step(String name, Class<R> type, Callable<R> body) throws Exception;

    /** {@link #step(String, Class, Callable)} for a step without a result. */
    void step(String name, Step body) throws Exception;

    @FunctionalInterface
    interface Step {
        void run() throws Exception;
    }
}
