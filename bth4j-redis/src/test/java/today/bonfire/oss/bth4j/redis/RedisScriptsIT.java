package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * The script efficiency changes of {@link RedisTaskStore}: step and failure markers read by CLAIM, head-first
 * PROMOTE, chunked renew, paged purgeWorkers and pipelines after SCRIPT FLUSH. Needs Docker.
 */
@Testcontainers
class RedisScriptsIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(RedisImage.NAME)
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final AtomicInteger NAMESPACES = new AtomicInteger();
    private static final Map<String, StepResult> STEPS =
            Map.of("a", new StepResult(Instant.ofEpochMilli(1_800_000_000_000L), "{\"x\":1}"));

    private static RedisClient jedis;

    private RedisTaskStore store;
    private Keys keys;
    private int queue;
    private int type;

    @BeforeAll
    static void connect() {
        var clientConfig = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(3000)
                .clientName("bth4j-scripts-test")
                .build();
        jedis = RedisClient.builder()
                .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                .clientConfig(clientConfig)
                .poolConfig(ConnectionPoolConfig.builder()
                        .maxPoolSize(16)
                        .minPoolSize(1)
                        .waitingForObjectTimeout(Duration.ofSeconds(5))
                        .build())
                .build();
    }

    @AfterAll
    static void disconnect() {
        if (jedis != null) {
            jedis.close();
        }
    }

    @BeforeEach
    void fresh() {
        store = new RedisTaskStore(jedis, "S" + NAMESPACES.incrementAndGet());
        keys = store.keys();
        store.init();
        queue = store.queueId("default");
        type = store.typeId("send");
    }

    private TaskRecord.Builder task(long id) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queue)
                .typeId(type)
                .maxAttempts(30)
                .created(Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }

    private List<TaskRecord> claim(int count) {
        return store.claim(new ClaimRequest(List.of(queue), count, LEASE, 1, null));
    }

    private TaskRecord claimOne(long id) {
        assertThat(store.enqueue(task(id).build())).isEqualTo(EnqueueOutcome.STORED);
        return claim(1).getFirst();
    }

    private static FailureInfo failure(int attempt) {
        return new FailureInfo(
                attempt, Instant.now().truncatedTo(ChronoUnit.MILLIS), "java.lang.Boom", "boom " + attempt, null);
    }

    private String marker(long id, String field) {
        return jedis.hget(keys.task(id), field);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- markers for steps and failures --------------------------------------------------------------

    @Test
    void firstAttemptClaimCarriesNoStepsOrFailuresAndTheHashHasNoMarkers() {
        var t = claimOne(1);

        assertThat(t.steps()).isNull();
        assertThat(t.failures()).isNull();
        assertThat(jedis.hgetAll(keys.task(1))).doesNotContainKeys("hs", "fc");
    }

    @Test
    void stepsWrittenThenReadBackByTheClaimAfterAnAutomaticRetry() {
        var t = claimOne(1);
        assertThat(store.saveSteps(1, t.revision(), STEPS)).isTrue();
        assertThat(marker(1, "hs")).isEqualTo("1");
        assertThat(store.fail(1, t.revision(), failure(1), Instant.now().minusSeconds(1), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(marker(1, "fc")).isEqualTo("1");
        assertThat(store.promote(10).moved()).isEqualTo(1);

        var again = claim(1).getFirst();

        assertThat(again.steps()).isEqualTo(STEPS);
        assertThat(again.failures()).hasSize(1);
        assertThat(again.failures().getFirst().message()).isEqualTo("boom 1");
        assertThat(again.attempts()).isEqualTo(2);
    }

    @Test
    void stepsAndFailuresSurviveAnOperatorRetryOfADeadTask() {
        var t = claimOne(1);
        store.saveSteps(1, t.revision(), STEPS);
        assertThat(store.fail(1, t.revision(), failure(1), null, true, null)).isEqualTo(FailOutcome.DEAD);
        assertThat(marker(1, "fc")).isEqualTo("1");
        assertThat(marker(1, "hs")).isEqualTo("1");
        var dead = store.getTask(1).orElseThrow();
        assertThat(dead.state()).isEqualTo(TaskState.DEAD);

        assertThat(store.retry(1, dead.revision())).isEqualTo(RetryOutcome.RETRIED);
        var again = claim(1).getFirst();

        assertThat(again.attempts()).isEqualTo(1);
        assertThat(again.steps()).isEqualTo(STEPS);
        assertThat(again.failures()).hasSize(1);
    }

    @Test
    void failureWithoutStepsIsReadByTheNextClaimAndStepsStayAbsent() {
        var t = claimOne(1);
        store.fail(1, t.revision(), failure(1), Instant.now().minusSeconds(1), true, null);
        store.promote(10);

        var again = claim(1).getFirst();

        assertThat(again.steps()).isNull();
        assertThat(again.failures()).hasSize(1);
        assertThat(marker(1, "hs")).isNull();
    }

    @Test
    void clearingTheStepsRemovesTheMarkerAndTheNextClaimReadsNone() {
        var t = claimOne(1);
        store.saveSteps(1, t.revision(), STEPS);
        assertThat(store.saveSteps(1, t.revision(), null)).isTrue();
        assertThat(marker(1, "hs")).isNull();
        assertThat(jedis.exists(keys.steps(1))).isFalse();
        assertThat(store.unclaim(1, t.revision())).isTrue();

        assertThat(claim(1).getFirst().steps()).isNull();
    }

    @Test
    void failureListIsTrimmedToTheLastTwenty() {
        var rev = claimOne(1).revision();
        for (var attempt = 1; attempt <= 23; attempt++) {
            assertThat(store.fail(1, rev, failure(attempt), Instant.now().minusSeconds(1), true, null))
                    .isEqualTo(FailOutcome.RETRY);
            store.promote(10);
            rev = claim(1).getFirst().revision();
        }

        assertThat(jedis.llen(keys.failures(1))).isEqualTo(RedisScripts.MAX_FAILURES);
        var back = store.getTask(1).orElseThrow();
        assertThat(back.failures()).hasSize(RedisScripts.MAX_FAILURES);
        assertThat(back.failures().getFirst().message()).isEqualTo("boom 4");
        assertThat(back.failures().getLast().message()).isEqualTo("boom 23");
    }

    // ---- renew ---------------------------------------------------------------------------------------

    @Test
    void renewOfMoreIdsThanOneChunkReturnsStatusesInOrder() {
        var count = 600;
        var tasks = new ArrayList<TaskRecord>();
        for (var i = 1; i <= count; i++) {
            tasks.add(task(i).build());
        }
        store.enqueueAll(tasks);
        var claimed = claim(count);
        assertThat(claimed).hasSize(count);
        var refs = new ArrayList<LeaseRef>();
        for (var t : claimed) {
            refs.add(new LeaseRef(t.id(), t.revision() + (t.id() % 100 == 0 ? 1 : 0)));
        }
        refs.add(new LeaseRef(999_999, 0));

        var statuses = store.renew(refs, Duration.ofMinutes(5));

        assertThat(statuses).hasSize(count + 1);
        for (var i = 0; i < refs.size(); i++) {
            var id = refs.get(i).id();
            assertThat(statuses.get(i).id()).isEqualTo(id);
            assertThat(statuses.get(i).owned()).as("id %d", id).isEqualTo(id % 100 != 0 && id != 999_999);
        }
        var firstClaimed = String.valueOf(claimed.getFirst().id());
        assertThat(jedis.zscore(keys.leased, firstClaimed))
                .isGreaterThan((double) Instant.now().plusSeconds(200).toEpochMilli());
    }

    // ---- purgeWorkers --------------------------------------------------------------------------------

    @Test
    void purgeWorkersRemovesMoreStaleWorkersThanOnePageAndReturnsTheTotal() {
        var stale = 1250;
        var old = (double) Instant.now().minusSeconds(3600).toEpochMilli();
        var scores = new HashMap<String, Double>();
        var ids = new ArrayList<Integer>();
        for (var i = 0; i < stale; i++) {
            var id = store.allocateWorkerId("w" + i);
            ids.add(id);
            scores.put(String.valueOf(id), old);
        }
        var fresh = store.allocateWorkerId("fresh");
        jedis.zadd(keys.workers, scores);

        assertThat(store.purgeWorkers(Duration.ofMinutes(10))).isEqualTo(stale);

        assertThat(jedis.zcard(keys.workers)).isEqualTo(1);
        assertThat(jedis.exists(keys.worker(fresh))).isTrue();
        for (var id : ids) {
            assertThat(jedis.exists(keys.worker(id))).as("worker %d", id).isFalse();
        }
        assertThat(store.purgeWorkers(Duration.ofMinutes(10))).isZero();
    }

    @Test
    void purgeWorkersWithExactlyOnePageEndsAfterAnEmptyPage() {
        var old = (double) Instant.now().minusSeconds(3600).toEpochMilli();
        var scores = new HashMap<String, Double>();
        for (var i = 0; i < RedisScripts.PURGE_WORKERS_PAGE; i++) {
            scores.put(String.valueOf(store.allocateWorkerId("w" + i)), old);
        }
        jedis.zadd(keys.workers, scores);

        assertThat(store.purgeWorkers(Duration.ofMinutes(10))).isEqualTo(RedisScripts.PURGE_WORKERS_PAGE);
        assertThat(jedis.zcard(keys.workers)).isZero();
    }

    // ---- script cache --------------------------------------------------------------------------------

    @Test
    void batchAfterScriptFlushLoadsOnceAndKeepsOrder() {
        store.enqueue(task(10).build());
        store.enqueue(task(20).build());
        jedis.scriptFlush();
        var batch = new ArrayList<TaskRecord>();
        for (var id = 1; id <= 40; id++) {
            batch.add(task(id).build());
        }

        var outcomes = store.enqueueAll(batch);

        assertThat(outcomes).hasSize(40);
        for (var i = 0; i < 40; i++) {
            var id = i + 1;
            assertThat(outcomes.get(i))
                    .as("id %d", id)
                    .isEqualTo(id == 10 || id == 20 ? EnqueueOutcome.EXISTS : EnqueueOutcome.STORED);
        }
        assertThat(jedis.exists(keys.task(40))).isTrue();
    }

    @Test
    void renewAfterScriptFlushStillReturnsEveryStatusInOrder() {
        var a = claimOne(1);
        store.enqueue(task(2).build());
        var b = claim(1).getFirst();
        jedis.scriptFlush();

        var statuses = store.renew(
                List.of(
                        new LeaseRef(2, b.revision()),
                        new LeaseRef(1, a.revision() + 1),
                        new LeaseRef(1, a.revision())),
                LEASE);

        assertThat(statuses).extracting(s -> s.id()).containsExactly(2L, 1L, 1L);
        assertThat(statuses).extracting(s -> s.owned()).containsExactly(true, false, true);
    }

    // ---- promote -------------------------------------------------------------------------------------

    @Test
    void promoteWithManyEmptyQueuesMovesTheDueTaskAndReportsTheNextDue() {
        for (var i = 0; i < 200; i++) {
            store.queueId("empty" + i);
        }
        var later = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
        var laterQueue = store.queueId("later");
        store.enqueue(task(1).due(Instant.now().plusMillis(300)).build());
        store.enqueue(task(2).queueId(laterQueue).due(later).build());
        sleep(450);

        var first = store.promote(100);

        assertThat(first.moved()).isEqualTo(1);
        assertThat(first.nextDue()).isEqualTo(later);
        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        var idle = store.promote(100);
        assertThat(idle.moved()).isZero();
        assertThat(idle.nextDue()).isEqualTo(later);
    }

    @Test
    void promoteOfOnlyEmptyQueuesMovesNothingAndHasNoNextDue() {
        for (var i = 0; i < 50; i++) {
            store.queueId("empty" + i);
        }

        var r = store.promote(100);

        assertThat(r.moved()).isZero();
        assertThat(r.nextDue()).isNull();
    }

    @Test
    void promoteKeepsItsLimitAndReportsTheStillDueHeadAsNextDue() {
        var other = store.queueId("other");
        store.enqueue(task(1).due(Instant.now().plusMillis(200)).build());
        store.enqueue(task(2).queueId(other).due(Instant.now().plusMillis(200)).build());
        store.enqueue(task(3).due(Instant.now().plusMillis(200)).build());
        sleep(350);

        var r = store.promote(2);

        assertThat(r.moved()).isEqualTo(2);
        assertThat(r.nextDue()).isNotNull().isBeforeOrEqualTo(Instant.now());
        assertThat(store.promote(2).moved()).isEqualTo(1);
        assertThat(store.promote(2).nextDue()).isNull();
    }
}
