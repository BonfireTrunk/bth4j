package today.bonfire.oss.bth4j.store;

/**
 * A recurring definition as the store keeps it. Each occurrence becomes a task with the definition's
 * event, account, queue, data and maximum attempts.
 *
 * @param revision changes on every {@link TaskStore#putRecurring}; a commit for an older revision is rejected
 */
public record RecurringDefinition(
        String scheduleId,
        String event,
        String account,
        String queue,
        String cron,
        String data,
        int maxAttempts,
        long revision) {}
