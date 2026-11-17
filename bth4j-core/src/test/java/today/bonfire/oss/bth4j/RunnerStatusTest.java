package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;

class RunnerStatusTest {

    private final StepClock clock = new StepClock();
    private final RunnerStatus status = new RunnerStatus(clock);

    @Test
    void theSameErrorReportedByEveryTaskOfAFailedBatchIsOneIssue() {
        var batchFailure = new TaskStoreException("connection reset");

        assertThat(status.storeError(batchFailure)).isTrue();
        assertThat(status.storeError(batchFailure)).isFalse();
        assertThat(status.storeError(batchFailure)).isFalse();

        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR));
    }

    @Test
    void anErrorTheBatchWrapsOncePerTaskIsOneIssue() {
        var error = new StackOverflowError("deep");

        assertThat(status.storeError(error)).isTrue();
        assertThat(status.storeError(new TaskStoreException("acknowledgement batch failed", error)))
                .isFalse();
        assertThat(status.storeError(new TaskStoreException("acknowledgement batch failed", error)))
                .isFalse();

        assertThat(status.issues()).hasSize(1);
    }

    @Test
    void distinctErrorsAreEachAnIssueAndTheLatestIsTheLastStoreError() {
        assertThat(status.storeError(new TaskStoreException("first"))).isTrue();
        assertThat(status.storeError(new TaskStoreException("second"))).isTrue();

        assertThat(status.issues()).hasSize(2);
        assertThat(status.lastStoreError()).contains("second");
    }

    @Test
    void anOutageRepeatingTheSameErrorIsOneIssueWithACountAndTheLatestTime() {
        assertThat(status.storeError(new TaskStoreException("store down"))).isTrue();
        clock.advance(Duration.ofSeconds(5));
        assertThat(status.storeError(new TaskStoreException("store down"))).isFalse();
        clock.advance(Duration.ofSeconds(5));
        assertThat(status.storeError(new TaskStoreException("store down"))).isFalse();

        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR);
            assertThat(i.at()).isEqualTo(Instant.EPOCH.plusSeconds(10));
            assertThat(i.detail()).contains("store down").contains("3 times");
        });
    }

    @Test
    void aRepeatedErrorMovesToTheEndAndOtherIssuesAreNotPushedOut() {
        status.storeError(new TaskStoreException("store down"));
        for (int i = 0; i < RunnerStatus.MAX_ISSUES + 50; i++) {
            status.storeError(new TaskStoreException("store down"));
            if (i % 10 == 0) status.issue(Issue.Kind.LEASE_LOST, "lease not renewed", (long) i);
        }

        assertThat(status.issues()).hasSizeLessThanOrEqualTo(RunnerStatus.MAX_ISSUES);
        assertThat(status.issues())
                .filteredOn(i -> i.kind() == Issue.Kind.STORE_ERROR)
                .hasSize(1);
        assertThat(status.issues())
                .filteredOn(i -> i.kind() == Issue.Kind.LEASE_LOST)
                .hasSizeGreaterThan(10);
    }

    @Test
    void differentMessagesOfTheSameClassStayDistinctIssues() {
        assertThat(status.storeError(new TaskStoreException("a"))).isTrue();
        assertThat(status.storeError(new TaskStoreException("b"))).isTrue();
        assertThat(status.storeError(new IllegalStateException("a"))).isTrue();

        assertThat(status.issues()).hasSize(3);
    }

    private static final class StepClock extends Clock {
        private Instant now = Instant.EPOCH;

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
