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
                .tags("type", "send", "queue", "emails", "result", result, "outcome", outcome)
                .counter()
                .count();
    }

    @Test
    void succeededCountsAndTimes() {
        observer.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(250), Outcome.APPLIED));

        assertThat(count("succeeded", "applied")).isEqualTo(1);
        var timer = registry.get(MicrometerTaskObserver.DURATION)
                .tags("type", "send", "queue", "emails")
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
    void unknownOutcomeIsTaggedUnknown() {
        observer.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(1), Outcome.UNKNOWN));
        observer.onEvent(new TaskEvent.Retrying(task(), new RuntimeException(), Instant.now(), Outcome.UNKNOWN));

        assertThat(count("succeeded", "unknown")).isEqualTo(1);
        assertThat(count("retried", "unknown")).isEqualTo(1);
        assertThat(registry.find(MicrometerTaskObserver.TASKS)
                        .tags("outcome", "lost")
                        .counter())
                .isNull();
    }

    @Test
    void leaseExpiredHasUnknownTypeAndQueue() {
        observer.onEvent(new TaskEvent.LeaseExpired(9, 1, 3, false));

        var counter = registry.get(MicrometerTaskObserver.TASKS)
                .tags("type", "unknown", "queue", "unknown", "result", "lease_lost", "outcome", "applied")
                .counter();
        assertThat(counter.count()).isEqualTo(1);
    }

    @Test
    void deferredScheduleFiredAndPurged() {
        observer.onEvent(new TaskEvent.Deferred(1, "send", "no processor"));
        observer.onEvent(new TaskEvent.ScheduleFired("nightly", 2, Instant.now()));
        observer.onEvent(new TaskEvent.Purged(TaskState.SUCCEEDED, 5));

        assertThat(registry.get(MicrometerTaskObserver.DEFERRED)
                        .tag("type", "send")
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
    void expiredHasUnknownTypeAndQueue() {
        observer.onEvent(new TaskEvent.Expired(9));

        var counter = registry.get(MicrometerTaskObserver.TASKS)
                .tags("type", "unknown", "queue", "unknown", "result", "expired", "outcome", "applied")
                .counter();
        assertThat(counter.count()).isEqualTo(1);
    }

    @Test
    void everyResultKindIsTaggedWithEveryOutcome() {
        for (var outcome : Outcome.values()) {
            observer.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(1), outcome));
            observer.onEvent(new TaskEvent.Retrying(task(), new RuntimeException("x"), Instant.now(), outcome));
            observer.onEvent(new TaskEvent.Died(task(), new RuntimeException("x"), "r", outcome));
            observer.onEvent(new TaskEvent.Rescheduled(task(), Instant.now(), outcome));
            observer.onEvent(new TaskEvent.Cancelled(task(), "why", outcome));
        }

        assertThat(Outcome.values()).containsExactlyInAnyOrder(Outcome.APPLIED, Outcome.LOST, Outcome.UNKNOWN);
        for (var result : new String[] {"succeeded", "retried", "died", "rescheduled", "cancelled"}) {
            for (var outcome : new String[] {"applied", "lost", "unknown"}) {
                assertThat(count(result, outcome)).as(result + "/" + outcome).isEqualTo(1);
            }
        }
        assertThat(registry.find(MicrometerTaskObserver.TASKS).counters()).hasSize(15);
    }

    private static final class FailingRegistry extends SimpleMeterRegistry {
        private final boolean failCounters;
        private final boolean failTimers;
        int counterCalls;
        int timerCalls;

        FailingRegistry(boolean failCounters, boolean failTimers) {
            this.failCounters = failCounters;
            this.failTimers = failTimers;
        }

        @Override
        public Counter counter(String name, Iterable<Tag> tags) {
            counterCalls++;
            if (failCounters) {
                throw new IllegalStateException("registry down");
            }
            return super.counter(name, tags);
        }

        @Override
        public Timer timer(String name, Iterable<Tag> tags) {
            timerCalls++;
            if (failTimers) {
                throw new IllegalStateException("registry down");
            }
            return super.timer(name, tags);
        }
    }

    @Test
    void counterFailureDoesNotPropagate() {
        var failing = new FailingRegistry(true, false);
        var safe = new MicrometerTaskObserver(failing);

        assertThatCode(() -> {
                    safe.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(1), Outcome.APPLIED));
                    safe.onEvent(new TaskEvent.Purged(TaskState.DEAD, 1));
                })
                .doesNotThrowAnyException();
        assertThat(failing.counterCalls).isEqualTo(2);
    }

    @Test
    void timerFailureDoesNotPropagateAndTheCountStillLands() {
        var failing = new FailingRegistry(false, true);
        var safe = new MicrometerTaskObserver(failing);

        assertThatCode(() -> safe.onEvent(new TaskEvent.Succeeded(task(), Duration.ofMillis(1), Outcome.APPLIED)))
                .doesNotThrowAnyException();

        assertThat(failing.timerCalls).isEqualTo(1);
        assertThat(failing.get(MicrometerTaskObserver.TASKS).counter().count()).isEqualTo(1);
    }
}
