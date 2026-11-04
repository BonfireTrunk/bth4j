package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

class BackgroundRunnerBuilderTest {

    @Test
    void defaultMaxRunTimeMustBePositiveAndAtMostMaxRunTime() {
        var builder = BackgroundRunner.builder();

        assertThatThrownBy(() -> builder.defaultMaxRunTime(Duration.ZERO)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.defaultMaxRunTime(TaskRequest.MAX_RUN_TIME.plusMillis(1)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.defaultMaxRunTime(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatCode(() -> builder.defaultMaxRunTime(TaskRequest.MAX_RUN_TIME))
                .doesNotThrowAnyException();
    }
}
