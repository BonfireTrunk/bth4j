package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

class BackgroundRunnerBuilderTest {

    @Test
    void defaultMaxRunTimeMustBePositiveAndAtMostMaxRunTime() {
        var builder = BackgroundRunner.builder();

        assertThatThrownBy(() -> builder.defaultMaxRunTime(Duration.ZERO))
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> builder.defaultMaxRunTime(TaskRequest.MAX_RUN_TIME.plusMillis(1)))
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> builder.defaultMaxRunTime(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(TaskConfigurationException.class);
        assertThatCode(() -> builder.defaultMaxRunTime(TaskRequest.MAX_RUN_TIME))
                .doesNotThrowAnyException();
    }

    @Test
    void aWorkerLabelOrQueueNameWithAU0000IsRejected() {
        var builder = BackgroundRunner.builder();

        assertThatThrownBy(() -> builder.workerLabel("host\u00001")).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> builder.configureQueues(List.of("Q\u00001"), null, null))
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> TaskClient.builder().queues(List.of("Q\u00001"), null))
                .isInstanceOf(TaskConfigurationException.class);
    }
}
