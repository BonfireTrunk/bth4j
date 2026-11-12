package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
    void durationsThatBecomeStoredTimesAreAtMostOneHundredYears() {
        var builder = BackgroundRunner.builder();
        var longest = TaskRequest.MAX_TIME_FROM_NOW;
        List<Function<Duration, BackgroundRunner.Builder>> settings = List.of(
                builder::leaseDuration,
                builder::succeededRetention,
                builder::deadRetention,
                builder::cancelledRetention,
                builder::runnerStaleTimeout);

        for (var setting : settings) {
            assertThatCode(() -> setting.apply(longest)).doesNotThrowAnyException();
            assertThatThrownBy(() -> setting.apply(longest.plusMillis(1)))
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("at most");
        }
    }

    @Test
    void everyIntervalHasAShortestValueAndIsAtMostOneHundredYears() {
        var builder = BackgroundRunner.builder();
        Map<Duration, List<Function<Duration, BackgroundRunner.Builder>>> settingsByShortest = Map.of(
                Duration.ofMillis(1), List.of(builder::purgeInterval, builder::serviceCheckInterval),
                Duration.ofMillis(10),
                        List.of(
                                builder::idlePollInterval,
                                builder::scheduledTasksCheckInterval,
                                builder::recurringTasksCheckInterval),
                Duration.ofSeconds(1), List.of(builder::maintenanceCheckInterval));

        settingsByShortest.forEach((shortest, settings) -> {
            for (var setting : settings) {
                assertThatCode(() -> setting.apply(shortest)).doesNotThrowAnyException();
                assertThatCode(() -> setting.apply(TaskRequest.MAX_TIME_FROM_NOW))
                        .doesNotThrowAnyException();
                for (Duration bad :
                        new Duration[] {null, Duration.ZERO, shortest.minusNanos(1), Duration.ofMillis(-1)}) {
                    assertThatThrownBy(() -> setting.apply(bad))
                            .as("%s", bad)
                            .isInstanceOf(TaskConfigurationException.class)
                            .hasMessageContaining("at least " + shortest.toMillis() + " ms");
                }
                // too long to count in milliseconds: refused, not an ArithmeticException
                for (Duration bad :
                        new Duration[] {TaskRequest.MAX_TIME_FROM_NOW.plusMillis(1), Duration.ofSeconds(Long.MAX_VALUE)
                        }) {
                    assertThatThrownBy(() -> setting.apply(bad))
                            .as("%s", bad)
                            .isInstanceOf(TaskConfigurationException.class)
                            .hasMessageContaining("at most");
                }
            }
        });
    }

    @Test
    void aRunnerLabelOrQueueNameWithAU0000IsRejected() {
        var builder = BackgroundRunner.builder();

        assertThatThrownBy(() -> builder.runnerLabel("host\u00001")).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Queues.of("Q\u00001")).isInstanceOf(TaskConfigurationException.class);
    }
}
