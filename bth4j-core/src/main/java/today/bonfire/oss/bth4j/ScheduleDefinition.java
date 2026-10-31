package today.bonfire.oss.bth4j;

import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * A recurring task: every time the cron expression fires, a task of the type with the payload is created.
 *
 * @param id          identifies the definition; registering the same id again replaces it
 * @param type        the task type of every occurrence
 * @param payload     the payload every occurrence carries, or null
 * @param cron        5-field unix cron expression, evaluated in UTC
 * @param queue       the queue of every occurrence, or null for the default queue
 * @param account     application-defined owner, or null
 * @param maxAttempts attempts per occurrence, or null for the default
 */
public record ScheduleDefinition<T>(
        String id, TaskType<T> type, T payload, String cron, String queue, String account, Integer maxAttempts) {

    public ScheduleDefinition {
        if (StringUtils.isBlank(id) || id.length() > 128) {
            throw new TaskValidationException("schedule id must be 1 to 128 characters");
        }
        if (type == null) throw new TaskValidationException("type is required");
        if (StringUtils.isBlank(cron)) throw new TaskValidationException("cron is required");
        if (type.hasPayload() && payload == null) {
            throw new TaskValidationException("Task type " + type + " needs a payload");
        }
        if (maxAttempts != null && (maxAttempts < 1 || maxAttempts > Short.MAX_VALUE)) {
            throw new TaskValidationException("maxAttempts must be between 1 and " + Short.MAX_VALUE);
        }
    }

    public static <T> ScheduleDefinition<T> of(String id, TaskType<T> type, T payload, String cron) {
        return new ScheduleDefinition<>(id, type, payload, cron, null, null, null);
    }

    public static ScheduleDefinition<Void> of(String id, TaskType<Void> type, String cron) {
        return new ScheduleDefinition<>(id, type, null, cron, null, null, null);
    }

    public ScheduleDefinition<T> queue(String queue) {
        return new ScheduleDefinition<>(id, type, payload, cron, queue, account, maxAttempts);
    }

    public ScheduleDefinition<T> account(String account) {
        return new ScheduleDefinition<>(id, type, payload, cron, queue, account, maxAttempts);
    }

    public ScheduleDefinition<T> maxAttempts(int maxAttempts) {
        return new ScheduleDefinition<>(id, type, payload, cron, queue, account, maxAttempts);
    }
}
