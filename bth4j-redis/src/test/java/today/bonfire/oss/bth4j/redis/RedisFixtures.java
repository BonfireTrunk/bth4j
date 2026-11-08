package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.UnifiedJedis;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/** Redis test fixtures: task history keys, checks that every trace of a task is gone, and aging of due times. */
final class RedisFixtures {

    private static final Map<String, StepResult> STEPS =
            Map.of("a", new StepResult(Instant.ofEpochMilli(1_800_000_000_000L), "{\"x\":1}"));

    private RedisFixtures() {}

    /**
     * Saves steps on a leased task and fails it for a retry at the given time, so it waits again with a step key
     * and a failure list. A payload key exists when the task was enqueued with a payload.
     */
    static void addHistory(RedisTaskStore store, TaskRecord leased, Instant retryAt) {
        assertThat(store.saveSteps(leased.id(), leased.revision(), STEPS)).isTrue();
        var failure = new FailureInfo(1, Instant.now().truncatedTo(ChronoUnit.MILLIS), "java.lang.Boom", "boom", null);
        assertThat(store.fail(leased.id(), leased.revision(), failure, retryAt, true, null))
                .isEqualTo(FailOutcome.RETRY);
    }

    /** Adds the history, moves the task back to its queue and leases it again. */
    static TaskRecord leasedWithHistory(RedisTaskStore store, int queue, TaskRecord leased) {
        addHistory(store, leased, Instant.now().minusSeconds(5));
        assertThat(store.promote(10).moved()).isEqualTo(1);
        return store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, Duration.ofSeconds(30), 1, null))
                .getFirst();
    }

    /** Moves the due time of a scheduled task, in its hash and in the scheduled zset, to the given instant. */
    static void ageDue(UnifiedJedis jedis, Keys keys, int queue, long id, Instant due) {
        jedis.zadd(keys.scheduled(queue), due.toEpochMilli(), String.valueOf(id));
        jedis.hset(keys.task(id), "due", String.valueOf(due.toEpochMilli()));
    }

    static void assertPopulated(UnifiedJedis jedis, Keys keys, long id) {
        assertThat(jedis.exists(keys.task(id))).as("hash of %d", id).isTrue();
        assertThat(jedis.exists(keys.data(id))).as("payload of %d", id).isTrue();
        assertThat(jedis.exists(keys.steps(id))).as("steps of %d", id).isTrue();
        assertThat(jedis.exists(keys.failures(id))).as("failures of %d", id).isTrue();
    }

    static void assertGone(UnifiedJedis jedis, Keys keys, long id) {
        assertThat(jedis.exists(keys.task(id), keys.data(id), keys.steps(id), keys.failures(id)))
                .as("keys of %d", id)
                .isZero();
        var member = String.valueOf(id);
        for (var key : jedis.keys(keys.prefix + "*")) {
            if ("zset".equals(jedis.type(key))) {
                assertThat(jedis.zscore(key, member)).as("%s holds %d", key, id).isNull();
            }
        }
    }
}
