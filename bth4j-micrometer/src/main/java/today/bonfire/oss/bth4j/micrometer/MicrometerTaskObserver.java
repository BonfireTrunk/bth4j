package today.bonfire.oss.bth4j.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Locale;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.TaskObserver;
import today.bonfire.oss.bth4j.TaskView;

/**
 * Turns {@link TaskEvent}s into Micrometer meters. Tags are limited to the configured queue and type names, the
 * result and the outcome; task ids, messages and error texts are never used as tags, so the number of meters stays
 * bounded. Register it with {@code BackgroundRunner.builder().observer(...)}.
 * <p>
 * Meters: {@code bth4j.tasks} (type, queue, result, outcome), {@code bth4j.task.duration} (type, queue),
 * {@code bth4j.deferred} (type, queue), {@code bth4j.schedule.fired} and {@code bth4j.purged} (state).
 */
public final class MicrometerTaskObserver implements TaskObserver {

    static final String TASKS = "bth4j.tasks";
    static final String DURATION = "bth4j.task.duration";
    static final String DEFERRED = "bth4j.deferred";
    static final String SCHEDULE_FIRED = "bth4j.schedule.fired";
    static final String PURGED = "bth4j.purged";

    private final MeterRegistry registry;
    private final Tags commonTags;

    /**
     * @param registry where the meters are registered
     * @param commonTags added to every meter this observer creates
     */
    public MicrometerTaskObserver(MeterRegistry registry, Tags commonTags) {
        this.registry = registry;
        this.commonTags = commonTags == null ? Tags.empty() : commonTags;
    }

    /** Same as {@link #MicrometerTaskObserver(MeterRegistry, Tags)} without common tags. */
    public MicrometerTaskObserver(MeterRegistry registry) {
        this(registry, Tags.empty());
    }

    /** Records the event; a failing registry is swallowed so metrics can never disturb the runner. */
    @Override
    public void onEvent(TaskEvent event) {
        try {
            record(event);
        } catch (RuntimeException e) {
            // observers must not break the runner; the event is dropped
        }
    }

    private void record(TaskEvent event) {
        switch (event) {
            case TaskEvent.Started e -> {}
            case TaskEvent.Succeeded e -> {
                countTask(e.task(), "succeeded", e.outcome());
                registry.timer(
                                DURATION,
                                commonTags.and(
                                        "type",
                                        e.task().type(),
                                        "queue",
                                        e.task().queue()))
                        .record(e.runTime());
            }
            case TaskEvent.Retrying e -> countTask(e.task(), "retried", e.outcome());
            case TaskEvent.Died e -> countTask(e.task(), "died", e.outcome());
            case TaskEvent.Rescheduled e -> countTask(e.task(), "rescheduled", e.outcome());
            case TaskEvent.Cancelled e -> countTask(e.task(), "cancelled", e.outcome());
            case TaskEvent.LeaseExpired e -> count(e.type(), e.queue(), "lease_lost", TaskEvent.Outcome.APPLIED);
            case TaskEvent.Expired e -> count(e.type(), e.queue(), "expired", TaskEvent.Outcome.APPLIED);
            case TaskEvent.Deferred e ->
                registry.counter(DEFERRED, commonTags.and("type", e.type(), "queue", e.queue()))
                        .increment();
            case TaskEvent.ScheduleFired e ->
                registry.counter(SCHEDULE_FIRED, commonTags).increment();
            case TaskEvent.Purged e ->
                registry.counter(
                                PURGED, commonTags.and("state", e.state().name().toLowerCase(Locale.ROOT)))
                        .increment(e.count());
            default -> {}
        }
    }

    private void countTask(TaskView task, String result, TaskEvent.Outcome outcome) {
        count(task.type(), task.queue(), result, outcome);
    }

    private void count(String type, String queue, String result, TaskEvent.Outcome outcome) {
        var tags = commonTags.and(
                "type", type,
                "queue", queue,
                "result", result,
                "outcome", outcome.name().toLowerCase(Locale.ROOT));
        registry.counter(TASKS, tags).increment();
    }
}
