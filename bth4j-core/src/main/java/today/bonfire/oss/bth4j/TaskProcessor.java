package today.bonfire.oss.bth4j;

/**
 * Runs tasks of one {@link TaskType}. Delivery is at least once: a task runs again when the worker holding
 * it died or lost its lease, so the work must be safe to repeat, or split into {@link TaskContext#step steps}.
 * <p>
 * Returning normally completes the task. Throwing {@link today.bonfire.oss.bth4j.exceptions.TaskRescheduleException}
 * runs it again later without counting the attempt; throwing
 * {@link today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException} ends it as dead; any other exception
 * counts as a failed attempt and the retry policy decides what happens next.
 *
 * @param <T> the payload type; {@link Void} for a task without payload
 */
@FunctionalInterface
public interface TaskProcessor<T> {

    void process(TaskContext context, T payload) throws Exception;
}
