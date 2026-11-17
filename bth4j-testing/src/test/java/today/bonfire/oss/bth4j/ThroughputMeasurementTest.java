package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.spi.TaskRecord;

class ThroughputMeasurementTest {

    @Test
    void processorCallbackDoesNotConfirmCompletionBeforeAppliedAcknowledgment() {
        var ticker = new AtomicLong(100);
        var measurement = new ThroughputContract.Measurement(Set.of(1L), 50, ticker::get);
        var task = task(1);
        measurement.start();
        ticker.set(200);
        measurement.onEvent(new TaskEvent.Started(task, 1));
        ticker.set(300);
        measurement.processorCallback(1);

        assertThatThrownBy(measurement::assertComplete)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("confirmed terminal acknowledgment IDs");
        assertThat(measurement.completionNanos).hasValue(0);

        ticker.set(900);
        measurement.onEvent(new TaskEvent.Succeeded(task, Duration.ZERO, TaskEvent.Outcome.APPLIED));

        measurement.assertComplete();
        assertThat(measurement.processorNanos).hasValue(200);
        assertThat(measurement.completionNanos).hasValue(800);
    }

    @Test
    void rejectsLostAndFailureEventsEvenWhenAnAppliedSuccessAlsoArrives() {
        var task = task(1);
        var error = new IllegalStateException("processor failed");
        var events = List.of(
                new TaskEvent.Succeeded(task, Duration.ZERO, TaskEvent.Outcome.LOST),
                new TaskEvent.Died(task, error, "failed", TaskEvent.Outcome.APPLIED),
                new TaskEvent.Retrying(task, error, Instant.EPOCH, TaskEvent.Outcome.APPLIED),
                new TaskEvent.Deferred(1, "q", "tp-void", "missing processor"));
        for (var event : events) {
            var measurement = new ThroughputContract.Measurement(Set.of(1L), 0, () -> 1L);
            measurement.start();
            measurement.onEvent(new TaskEvent.Started(task, 1));
            measurement.processorCallback(1);
            measurement.onEvent(event);
            measurement.onEvent(new TaskEvent.Succeeded(task, Duration.ZERO, TaskEvent.Outcome.APPLIED));

            assertThatThrownBy(measurement::assertComplete)
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("unexpected events");
        }
    }

    @Test
    void duplicateSuccessCannotSubstituteForAnotherAcceptedId() {
        var measurement = new ThroughputContract.Measurement(Set.of(1L, 2L), 0, () -> 1L);
        measurement.start();
        for (long id : List.of(1L, 2L)) {
            measurement.onEvent(new TaskEvent.Started(task(id), 1));
            measurement.processorCallback(id);
        }
        var success = new TaskEvent.Succeeded(task(1), Duration.ZERO, TaskEvent.Outcome.APPLIED);
        measurement.onEvent(success);
        measurement.onEvent(success);

        assertThatThrownBy(measurement::assertComplete)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("confirmed terminal acknowledgment IDs");
    }

    @Test
    void startLatencyUsesMonotonicBatchReturnTimeAndNearestRank() {
        var ticker = new AtomicLong(1_000_000);
        var measurement = new ThroughputContract.Measurement(Set.of(1L, 2L, 3L, 4L), ticker.get(), ticker::get);
        measurement.start();
        for (long id : List.of(1L, 2L, 3L, 4L)) {
            ticker.set(1_000_000 + id * 1_000_000);
            measurement.onEvent(new TaskEvent.Started(task(id), 1));
        }

        assertThat(measurement.percentileMillis(50)).isEqualTo(2.0);
        assertThat(measurement.percentileMillis(95)).isEqualTo(4.0);
        assertThat(measurement.percentileMillis(99)).isEqualTo(4.0);
    }

    private static TaskView task(long id) {
        return new TaskView(TaskRecord.builder().id(id).build(), "default", "tp-void");
    }
}
