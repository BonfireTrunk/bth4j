package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TestEvents;

/**
 * Tests of the parts of {@link TaskOps} that do not touch Redis.
 */
class TaskOpsTest {

    private final TaskOps ops = new TaskOps(null, null, null, null, null);

    @Test
    void retryCountIsExhaustedOnlyAboveTheEventRetryCount() {
        // COOL allows 3 retries
        assertThat(ops.isRetryCountExhausted(TestEvents.COOL, 1)).isFalse();
        assertThat(ops.isRetryCountExhausted(TestEvents.COOL, 3)).isFalse();
        assertThat(ops.isRetryCountExhausted(TestEvents.COOL, 4)).isTrue();
    }

    @Test
    void eventWithoutRetriesIsExhaustedOnFirstStaleCheck() {
        assertThat(ops.isRetryCountExhausted(TestEvents.REGULAR_TASK, 1)).isTrue();
    }

    @Test
    void dataForBlankIdOrMissingClassIsNullWithoutTouchingRedis() {
        assertThat(ops.getDataForTask((String) null)).isNull();
        assertThat(ops.getDataForTask("  ")).isNull();
        assertThat(ops.getDataForTask("", String.class)).isNull();
        assertThat(ops.getDataForTask("id", (Class<Object>) null)).isNull();
    }
}
