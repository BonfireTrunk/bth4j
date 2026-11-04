package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.util.Map;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.testing.InMemoryStore;

final class ScheduleFixtures {
    static final String SCHEDULE_ID = "sched";

    private ScheduleFixtures() {}

    /**
     * Registers a fixed-delay definition due now and commits its first occurrence the way the recurring service
     * does, so the occurrence is parked behind the definition until it ends. The occurrence is queued.
     *
     * @return the id of the occurrence
     */
    static long fixedDelayOccurrence(
            InMemoryStore store, TaskClient client, java.time.Instant now, int typeId, int queueId, int maxAttempts) {
        var definition = new RecurringDefinition(
                SCHEDULE_ID,
                typeId,
                0,
                queueId,
                null,
                null,
                null,
                null,
                0,
                60_000L,
                false,
                null,
                maxAttempts,
                null,
                false,
                0);
        store.putRecurring(definition, now, now);
        var due = store.dueRecurring(now, 10).getFirst();
        var occurrence = TaskRecord.builder()
                .id(client.nextId())
                .queueId(queueId)
                .typeId(typeId)
                .state(TaskState.QUEUED)
                .maxAttempts(maxAttempts)
                .createdAt(now)
                .due(now)
                .scheduleId(SCHEDULE_ID)
                .headers(Map.of(RecurringTaskService.FIXED_DELAY_HEADER, "PT1M"))
                .build();
        if (!store.commitRecurring(due, now.plus(Duration.ofDays(36_500)), occurrence)) {
            throw new IllegalStateException("the occurrence was not committed");
        }
        store.promote(10);
        return occurrence.id();
    }
}
