package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.RetryPolicy.Decision;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

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
}
