package today.bonfire.oss.bth4j.spi;

/**
 * A recurring definition as the store keeps it. Each occurrence becomes a task with the definition's event,
 * queue, account, payload and maximum attempts.
 *
 * @param revision changes on every {@link ScheduleStore#putRecurring}; a commit for an older revision is rejected
 */
public record RecurringDefinition(
        String scheduleId,
        int eventId,
        int payloadVersion,
        int queueId,
        String account,
        String cron,
        String data,
        int maxAttempts,
        long revision) {}
