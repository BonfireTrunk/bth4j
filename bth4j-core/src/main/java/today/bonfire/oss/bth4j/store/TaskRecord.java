package today.bonfire.oss.bth4j.store;

import java.time.Instant;

/**
 * A task as the store keeps it. Fields that do not apply to the task's state are null (or zero).
 *
 * @param id          unique across all states
 * @param event       the event's string value, parsed by the application's event parser
 * @param account     application-defined owner
 * @param queue       queue name
 * @param data        serialized payload, or null
 * @param attempts    how often the task was claimed
 * @param maxAttempts after this many failed attempts the task is dead
 * @param created     when the task was stored
 * @param due         when a scheduled task becomes runnable, null when queued
 * @param token       lease token of the worker that holds the task, null unless leased
 * @param leaseUntil  when the lease expires unless renewed, null unless leased
 * @param scheduleId  the recurring definition this task is an occurrence of, or null
 * @param error       message of the last failed attempt, or null
 * @param died        when the task was moved to the dead state, or null
 */
public record TaskRecord(
        String id,
        String event,
        String account,
        String queue,
        String data,
        int attempts,
        int maxAttempts,
        Instant created,
        Instant due,
        String token,
        Instant leaseUntil,
        String scheduleId,
        String error,
        Instant died) {

    /** A task that has not been stored yet. */
    public static TaskRecord newTask(
            String id, String event, String account, String queue, String data, int maxAttempts, Instant due) {
        return new TaskRecord(
                id, event, account, queue, data, 0, maxAttempts, Instant.now(), due, null, null, null, null, null);
    }

    public TaskRecord withScheduleId(String scheduleId) {
        return new TaskRecord(
                id,
                event,
                account,
                queue,
                data,
                attempts,
                maxAttempts,
                created,
                due,
                token,
                leaseUntil,
                scheduleId,
                error,
                died);
    }
}
