package today.bonfire.oss.bth4j.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.TaskEvent.Outcome;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

class MicrometerTaskObserverTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerTaskObserver observer = new MicrometerTaskObserver(registry);

    private static TaskView task() {
        var record = TaskRecord.builder().id(7).state(TaskState.LEASED).build();
        return new TaskView(record, "emails", "send");
    }

    private double count(String result, String outcome) {
        return registry.get(MicrometerTaskObserver.TASKS)
                .tags("event", "send", "queue", "emails", "result", result, "outcome", outcome)
                .counter()
                .count();
    }

    @Test
    void succeededCountsAndTimes() {
        observer.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(250), Outcome.APPLIED));

        assertThat(count("succeeded", "applied")).isEqualTo(1);
        var timer = registry.get(MicrometerTaskObserver.DURATION)
                .tags("event", "send", "queue", "emails")
                .timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(250);
    }

    @Test
    void retryingDiedRescheduledAndCancelledHaveTheirOwnResult() {
        observer.onEvent(new TaskEvent.Retrying(task(), new RuntimeException("x"), Instant.now(), Outcome.APPLIED));
        observer.onEvent(new TaskEvent.Died(task(), new RuntimeException("x"), "boom", Outcome.APPLIED));
        observer.onEvent(new TaskEvent.Rescheduled(task(), Instant.now(), Outcome.APPLIED));
        observer.onEvent(new TaskEvent.Cancelled(task(), "why", Outcome.APPLIED));

        assertThat(count("retried", "applied")).isEqualTo(1);
        assertThat(count("died", "applied")).isEqualTo(1);
        assertThat(count("rescheduled", "applied")).isEqualTo(1);
        assertThat(count("cancelled", "applied")).isEqualTo(1);
    }

    @Test
    void lostOutcomeIsTaggedLost() {
        observer.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(1), Outcome.LOST));
        observer.onEvent(new TaskEvent.Died(task(), new RuntimeException(), "r", Outcome.LOST));

        assertThat(count("succeeded", "lost")).isEqualTo(1);
        assertThat(count("died", "lost")).isEqualTo(1);
        assertThat(registry.find(MicrometerTaskObserver.TASKS)
                        .tags("outcome", "applied")
                        .counter())
                .isNull();
    }

    @Test
    void leaseExpiredHasUnknownEventAndQueue() {
        observer.onEvent(new TaskEvent.LeaseExpired(9, 1, 3, false));

        var counter = registry.get(MicrometerTaskObserver.TASKS)
                .tags("event", "unknown", "queue", "unknown", "result", "lease_lost", "outcome", "applied")
                .counter();
        assertThat(counter.count()).isEqualTo(1);
    }

    @Test
    void deferredScheduleFiredAndPurged() {
        observer.onEvent(new TaskEvent.Deferred(1, "send", "no processor"));
        observer.onEvent(new TaskEvent.ScheduleFired("nightly", 2, Instant.now()));
        observer.onEvent(new TaskEvent.Purged(TaskState.SUCCEEDED, 5));

        assertThat(registry.get(MicrometerTaskObserver.DEFERRED)
                        .tag("event", "send")
                        .counter()
                        .count())
                .isEqualTo(1);
        assertThat(registry.get(MicrometerTaskObserver.SCHEDULE_FIRED).counter().count())
                .isEqualTo(1);
        assertThat(registry.get(MicrometerTaskObserver.PURGED)
                        .tag("state", "succeeded")
                        .counter()
                        .count())
                .isEqualTo(5);
    }

    @Test
    void startedRegistersNothing() {
        observer.onEvent(new TaskEvent.Started(task(), 1));

        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void tagsNeverCarryIdsOrMessages() {
        observer.onEvent(new TaskEvent.Died(task(), new RuntimeException("secret"), "reason text", Outcome.APPLIED));

        var values = registry.getMeters().stream()
                .flatMap(m -> m.getId().getTags().stream())
                .map(Tag::getValue)
                .toList();
        assertThat(values).containsExactlyInAnyOrder("send", "emails", "died", "applied");
    }

    @Test
    void commonTagsAreAddedToEveryMeter() {
        var tagged = new MicrometerTaskObserver(registry, Tags.of("app", "billing"));

        tagged.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(1), Outcome.APPLIED));
        tagged.onEvent(new TaskEvent.ScheduleFired("s", 1, Instant.now()));

        assertThat(registry.getMeters())
                .hasSize(3)
                .allSatisfy(m -> assertThat(m.getId().getTag("app")).isEqualTo("billing"));
    }

    @Test
    void registryFailureDoesNotPropagate() {
        var failing = new SimpleMeterRegistry() {
            @Override
            public Counter counter(String name, Iterable<Tag> tags) {
                throw new IllegalStateException("registry down");
            }

            @Override
            public Timer timer(String name, Iterable<Tag> tags) {
                throw new IllegalStateException("registry down");
            }
        };
        var safe = new MicrometerTaskObserver(failing);

        assertThatCode(() -> {
                    safe.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(1), Outcome.APPLIED));
                    safe.onEvent(new TaskEvent.Purged(TaskState.DEAD, 1));
                })
                .doesNotThrowAnyException();
    }
}
