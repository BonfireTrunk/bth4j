package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

class RunnerStatusTest {

    private final RunnerStatus status = new RunnerStatus(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));

    @Test
    void theSameErrorReportedByEveryTaskOfAFailedBatchIsOneIssue() {
        var batchFailure = new TaskStoreException("connection reset");

        assertThat(status.storeError(batchFailure)).isTrue();
        assertThat(status.storeError(batchFailure)).isFalse();
        assertThat(status.storeError(batchFailure)).isFalse();

        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo("store-error"));
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
}
