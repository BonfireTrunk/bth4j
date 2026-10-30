package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import today.bonfire.oss.bth4j.JsonMapperTest;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.redis.Keys;
import today.bonfire.oss.bth4j.redis.RedisTaskStore;
import today.bonfire.oss.bth4j.store.FailOutcome;
import today.bonfire.oss.bth4j.store.TaskStore;

/** {@link TaskOpsContract} against a real Redis, plus the checks that read Redis keys directly. Needs Docker. */
@Testcontainers
class RedisTaskOpsIT extends TaskOpsContract {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static final Duration LEASE = Duration.ofSeconds(30);

    private static RedisClient jedis;

    @BeforeAll
    static void connect() {
        var poolConfig = ConnectionPoolConfig.builder()
                .maxPoolSize(8)
                .minPoolSize(1)
                .waitingForObjectTimeout(Duration.ofSeconds(5))
                .build();
        var clientConfig = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(3000)
                .clientName("bth4j-ops-test")
                .build();
        jedis = RedisClient.builder()
                .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                .clientConfig(clientConfig)
                .poolConfig(poolConfig)
                .build();
    }

    @AfterAll
    static void disconnect() {
        if (jedis != null) jedis.close();
    }

    @Override
    protected TaskStore newStore(String namespace) {
        return new RedisTaskStore(jedis, namespace);
    }

    private static TaskOps opsOn(RedisTaskStore store) {
        var queues = new QueuesHolder(Set.of("def", "hi"), List.of("def", "hi"), "def");
        return new TaskOps(store, new JsonMapperTest(), TestEvents.UNKNOWN::from, queues, Duration.ofDays(1));
    }

    private static Task.Builder task(String id, TestEvents event) {
        return Task.Builder.newTask(id).event(event).accountId(7);
    }

    private static Task claimOne(TaskOps ops, String id) {
        assertThat(ops.enqueue(task(id, TestEvents.REGULAR_TASK).build())).isTrue();
        return ops.claim(List.of("def"), 1, "tok", LEASE).getFirst();
    }

    @Test
    void claimedTaskIsInTheLeasedSet() {
        var store = new RedisTaskStore(jedis, "R-leased");
        var ops = opsOn(store);

        claimOne(ops, "a");

        assertThat(jedis.zrange(store.keys().leased, 0, -1)).containsExactly("a");
    }

    @Test
    void deadTaskKeepsItsErrorInTheHashAndExpires() {
        var store = new RedisTaskStore(jedis, "R-dead");
        var ops = opsOn(store);
        Keys keys = store.keys();
        var t = claimOne(ops, "a");

        assertThat(ops.fail(t, Duration.ofMillis(100), "boom")).isEqualTo(FailOutcome.DEAD);

        assertThat(jedis.hget(keys.task("a"), "error")).isEqualTo("boom");
        assertThat(jedis.ttl(keys.task("a"))).isPositive();
    }

    @Test
    void retryDeadTaskRemovesTheHashExpiry() {
        var store = new RedisTaskStore(jedis, "R-retry");
        var ops = opsOn(store);
        var t = claimOne(ops, "a");
        assertThat(ops.fail(t, Duration.ZERO, "boom")).isEqualTo(FailOutcome.DEAD);

        assertThat(ops.retryDeadTask("a")).isTrue();

        assertThat(jedis.ttl(store.keys().task("a"))).isEqualTo(-1);
    }

    @Test
    void cronCommitSchedulesTheOccurrenceAtItsFireTime() {
        var store = new RedisTaskStore(jedis, "R-cron");
        var ops = opsOn(store);
        ops.addRecurringTask(
                task("r", TestEvents.RECURRING).cronExpression("* * * * *").build());
        var due = ops.cronDue(Instant.now().plus(Duration.ofHours(1)), 10).getFirst();
        var next = Task.nextExecution("* * * * *", due.fireAt()).orElseThrow();

        assertThat(ops.cronCommit(due, next)).isTrue();

        assertThat(jedis.zscore(store.keys().scheduled, "r:" + due.fireAt().toEpochMilli()))
                .isEqualTo((double) due.fireAt().toEpochMilli());
    }

    @Test
    void deletedRecurringTaskLeavesNoDefinitionKey() {
        var store = new RedisTaskStore(jedis, "R-del");
        var ops = opsOn(store);
        ops.addRecurringTask(
                task("r", TestEvents.RECURRING).cronExpression("* * * * *").build());

        assertThat(ops.deleteRecurringTask("r")).isTrue();

        assertThat(jedis.exists(store.keys().cronDefinition("r"))).isFalse();
    }
}
