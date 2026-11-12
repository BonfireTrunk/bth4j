package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.RetryPolicy.Decision;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.spi.TaskRecord;

class RetryPolicyTest {

    private static final Throwable ERROR = new IllegalStateException("boom");

    private static TaskView failedTask(int attempts) {
        var record = TaskRecord.builder()
                .id(1)
                .state(TaskState.LEASED)
                .attempts(attempts)
                .maxAttempts(100)
                .build();
        return new TaskView(record, "Q:DEF", "event");
    }

    private static long delayMillis(RetryPolicy policy, int attempts) {
        var decision = policy.decide(failedTask(attempts), ERROR);
        assertThat(decision).isInstanceOf(Decision.RetryAfter.class);
        return ((Decision.RetryAfter) decision).delay().toMillis();
    }

    @Test
    void exponentialDoublesPerAttemptWithinTwentyPercentJitter() {
        var policy = RetryPolicy.exponential(Duration.ofSeconds(1), Duration.ofHours(1));

        for (int attempt = 1; attempt <= 6; attempt++) {
            long nominal = 1000L << (attempt - 1);
            for (int i = 0; i < 200; i++) {
                assertThat(delayMillis(policy, attempt))
                        .isBetween((long) (nominal * 0.8) - 1, (long) (nominal * 1.2) + 1);
            }
        }
    }

    @Test
    void exponentialNeverExceedsTheCapPlusJitter() {
        var policy = RetryPolicy.exponential(Duration.ofSeconds(1), Duration.ofSeconds(10));

        for (int i = 0; i < 200; i++) {
            assertThat(delayMillis(policy, 30)).isBetween(8_000L - 1, 12_000L + 1);
        }
    }

    @Test
    void exponentialTreatsZeroAttemptsAsTheFirst() {
        var policy = RetryPolicy.exponential(Duration.ofSeconds(1), Duration.ofHours(1));

        assertThat(delayMillis(policy, 0)).isBetween(799L, 1201L);
    }

    @Test
    void exponentialJitterActuallyVaries() {
        var policy = RetryPolicy.exponential(Duration.ofSeconds(10), Duration.ofHours(1));

        var distinct = java.util.stream.IntStream.range(0, 50)
                .mapToObj(i -> delayMillis(policy, 1))
                .distinct()
                .count();

        assertThat(distinct).isGreaterThan(1);
    }

    @Test
    void exponentialWithHugeAttemptCountDoesNotOverflow() {
        var policy = RetryPolicy.exponential(Duration.ofSeconds(1), Duration.ofHours(1));

        assertThat(delayMillis(policy, 30_000))
                .isBetween(0L, Duration.ofHours(1).toMillis() * 12 / 10);
    }

    @Test
    void defaultPolicyStartsAtAboutOneSecond() {
        assertThat(delayMillis(RetryPolicy.defaultPolicy(), 1)).isBetween(799L, 1201L);
    }

    @Test
    void fixedAlwaysReturnsTheSameDelay() {
        var policy = RetryPolicy.fixed(Duration.ofMillis(250));

        for (int attempt = 1; attempt <= 10; attempt++) {
            assertThat(delayMillis(policy, attempt)).isEqualTo(250L);
        }
    }

    @Test
    void decisionFactoriesNormalizeTheDelay() {
        assertThat(((Decision.RetryAfter) Decision.retryAfter(null)).delay()).isEqualTo(Duration.ZERO);
        assertThat(((Decision.RetryAfter) Decision.retryAfter(Duration.ofSeconds(-5))).delay())
                .isEqualTo(Duration.ZERO);
        assertThat(Decision.dead("no more")).isEqualTo(new Decision.Dead("no more"));
    }

    @Test
    void aDecisionBuiltWithoutTheFactoryNormalizesItsDelayToo() {
        assertThat(new Decision.RetryAfter(null).delay()).isEqualTo(Duration.ZERO);
        assertThat(new Decision.RetryAfter(Duration.ofSeconds(-5)).delay()).isEqualTo(Duration.ZERO);
        assertThat(new Decision.RetryAfter(Duration.ofSeconds(5)).delay()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void exponentialKeepsDoublingASmallBaseUntilTheCap() {
        var policy = RetryPolicy.exponential(Duration.ofMillis(10), Duration.ofDays(30));

        long nominal = 10L << 24;
        assertThat(delayMillis(policy, 25)).isBetween((long) (nominal * 0.8) - 1, (long) (nominal * 1.2) + 1);
        long cap = Duration.ofDays(30).toMillis();
        for (int attempts : new int[] {40, 64, 65, 1000}) {
            assertThat(delayMillis(policy, attempts))
                    .as("attempt %d", attempts)
                    .isBetween((long) (cap * 0.8) - 1, (long) (cap * 1.2) + 1);
        }
    }

    @Test
    void exponentialCountsACapBeyondOneHundredYearsAsOneHundredYears() {
        var policy = RetryPolicy.exponential(Duration.ofSeconds(1), Duration.ofSeconds(Long.MAX_VALUE));

        long longest = TaskRequest.MAX_TIME_FROM_NOW.toMillis();
        assertThat(delayMillis(policy, 200)).isBetween((long) (longest * 0.8) - 1, (long) (longest * 1.2) + 1);
    }

    @Test
    void exponentialAndFixedRefuseANullOrNegativeArgument() {
        var second = Duration.ofSeconds(1);
        for (Duration bad : new Duration[] {null, Duration.ofMillis(-1)}) {
            assertThatThrownBy(() -> RetryPolicy.exponential(bad, second))
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("base cannot be null or negative");
            assertThatThrownBy(() -> RetryPolicy.exponential(second, bad))
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("cap cannot be null or negative");
            assertThatThrownBy(() -> RetryPolicy.fixed(bad))
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("delay cannot be null or negative");
        }
    }
}
