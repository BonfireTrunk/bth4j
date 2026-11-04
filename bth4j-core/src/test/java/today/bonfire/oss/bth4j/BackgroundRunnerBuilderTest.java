package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

class BackgroundRunnerBuilderTest {

    @Test
    void defaultMaxRunMustBePositiveAndAtMostMaxRun() {
        var builder = BackgroundRunner.builder();

        assertThatThrownBy(() -> builder.defaultMaxRun(Duration.ZERO)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.defaultMaxRun(TaskRequest.MAX_RUN.plusMillis(1)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> builder.defaultMaxRun(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(TaskConfigurationError.class);
        assertThatCode(() -> builder.defaultMaxRun(TaskRequest.MAX_RUN)).doesNotThrowAnyException();
    }
}
