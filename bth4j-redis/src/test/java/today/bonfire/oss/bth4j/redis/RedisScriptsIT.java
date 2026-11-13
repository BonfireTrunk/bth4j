package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisNoScriptException;
import today.bonfire.oss.bth4j.EnqueueResult;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.JsonMapperTest;
import today.bonfire.oss.bth4j.Queues;
import today.bonfire.oss.bth4j.RetryOutcome;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskClient;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.TaskType;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * The script efficiency changes of {@link RedisTaskStore}: step and failure markers read by CLAIM, head-first
 * PROMOTE, chunked renew, paged purgeRunners and pipelines after SCRIPT FLUSH. Needs Docker.
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
    private RecordingJedis spy;
    private RedisTaskStore spied;
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
        store = RedisTaskStore.builder()
                .client(jedis)
                .namespace("S" + NAMESPACES.incrementAndGet())
                .build();
        keys = store.keys();
        store.init();
        queue = store.queueId("default");
        type = store.typeId("send");
        spy = RecordingJedis.connect(REDIS.getHost(), REDIS.getMappedPort(6379));
        spied = RedisTaskStore.builder().client(spy).namespace(keys.namespace).build();
    }

    @AfterEach
    void closeSpy() {
        spy.close();
    }

    private TaskRecord.Builder task(long id) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queue)
                .typeId(type)
                .maxAttempts(30)
                .createdAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }

    private List<TaskRecord> claim(int count) {
        return store.claim(new ClaimRequest(List.of(new QueueGrant(queue, count, count)), count, LEASE, 1, null));
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

    @Test
    void allGrantsAndBothPassesUseOneScriptCall() {
        var other = store.queueId("other");
        var request =
                new ClaimRequest(List.of(new QueueGrant(queue, 1, 3), new QueueGrant(other, 1, 1)), 4, LEASE, 1, null);
        spied.claim(request);
        spy.clearEvents();
        store.enqueue(task(1).dueAt(Instant.EPOCH).build());
        store.enqueue(task(2).dueAt(Instant.ofEpochMilli(1)).build());
        store.enqueue(task(3).dueAt(Instant.ofEpochMilli(2)).build());
        store.enqueue(task(4).queueId(other).build());

        assertThat(spied.claim(request)).extracting(TaskRecord::id).containsExactly(1L, 4L, 2L, 3L);
        assertThat(spy.events()).containsExactly("evalsha");
    }

    @Test
    void queuedByTypeUsesOneScriptAndOnlyQueueCardinalities() {
        var other = store.typeId("other");
        store.enqueue(task(1).build());
        store.enqueue(task(2).typeId(other).build());
        jedis.unlink(keys.task(1), keys.task(2));
        spied.queuedByType();
        spy.clearEvents();

        assertThat(spied.queuedByType()).isEqualTo(Map.of(queue, Map.of(type, 1L, other, 1L)));
        assertThat(spy.events()).containsExactly("evalsha");
        assertThat(store.counts().get(queue).queued()).isEqualTo(2);
    }

    @Test
    void heartbeatUsesOneScriptAndKeepsTheRunnerIndexAndHashTimeEqual() {
        var runner = store.allocateRunnerId("worker", Set.of(queue), Set.of(type));
        spied.heartbeat(runner, "worker", Set.of(queue), Set.of(type), 0, true);
        spy.clearEvents();

        spied.heartbeat(runner, "worker", Set.of(queue), Set.of(type), 4, false);

        assertThat(spy.events()).containsExactly("evalsha");
        assertThat(jedis.hget(keys.runner(runner), Keys.F_SEEN))
                .isEqualTo(String.valueOf(
                        jedis.zscore(keys.runners, String.valueOf(runner)).longValue()));
        assertThat(jedis.hget(keys.runner(runner), Keys.F_RUNNING)).isEqualTo("4");
        assertThat(jedis.hget(keys.runner(runner), Keys.F_ACCEPTING)).isEqualTo("0");
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
        var dead = store.task(1).orElseThrow();
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
        var back = store.task(1).orElseThrow();
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

    // ---- purgeRunners --------------------------------------------------------------------------------

    @Test
    void purgeRunnersRemovesMoreStaleRunnersThanOnePageAndReturnsTheTotal() {
        var stale = 1250;
        var old = (double) Instant.now().minusSeconds(3600).toEpochMilli();
        var scores = new HashMap<String, Double>();
        var ids = new ArrayList<Integer>();
        for (var i = 0; i < stale; i++) {
            var id = store.allocateRunnerId("w" + i, Set.of(), Set.of());
            ids.add(id);
            scores.put(String.valueOf(id), old);
        }
        var fresh = store.allocateRunnerId("fresh", Set.of(), Set.of());
        jedis.zadd(keys.runners, scores);

        assertThat(store.purgeRunners(Duration.ofMinutes(10))).isEqualTo(stale);

        assertThat(jedis.zcard(keys.runners)).isEqualTo(1);
        assertThat(jedis.exists(keys.runner(fresh))).isTrue();
        for (var id : ids) {
            assertThat(jedis.exists(keys.runner(id))).as("worker %d", id).isFalse();
        }
        assertThat(store.purgeRunners(Duration.ofMinutes(10))).isZero();
    }

    @Test
    void purgeRunnersWithExactlyOnePageEndsAfterAnEmptyPage() {
        var old = (double) Instant.now().minusSeconds(3600).toEpochMilli();
        var scores = new HashMap<String, Double>();
        for (var i = 0; i < RedisScripts.PURGE_RUNNERS_PAGE; i++) {
            scores.put(String.valueOf(store.allocateRunnerId("w" + i, Set.of(), Set.of())), old);
        }
        jedis.zadd(keys.runners, scores);

        assertThat(store.purgeRunners(Duration.ofMinutes(10))).isEqualTo(RedisScripts.PURGE_RUNNERS_PAGE);
        assertThat(jedis.zcard(keys.runners)).isZero();
    }

    // ---- script cache --------------------------------------------------------------------------------

    private List<TaskRecord> batch(int count) {
        var batch = new ArrayList<TaskRecord>();
        for (var id = 1; id <= count; id++) {
            batch.add(task(id).build());
        }
        return batch;
    }

    private void assertBatchOutcomes(List<EnqueueOutcome> outcomes, int count) {
        assertThat(outcomes).hasSize(count);
        for (var i = 0; i < count; i++) {
            var id = i + 1;
            assertThat(outcomes.get(i))
                    .as("id %d", id)
                    .isEqualTo(id == 10 || id == 20 ? EnqueueOutcome.EXISTS : EnqueueOutcome.STORED);
        }
    }

    @Test
    void batchAfterScriptFlushLoadsOnceAndKeepsOrder() {
        store.enqueue(task(10).build());
        store.enqueue(task(20).build());
        jedis.scriptFlush();

        var outcomes = spied.enqueueAll(batch(4 * RedisTaskStore.ENQUEUE_CHUNK));

        assertBatchOutcomes(outcomes, 4 * RedisTaskStore.ENQUEUE_CHUNK);
        assertThat(jedis.exists(keys.task(4 * RedisTaskStore.ENQUEUE_CHUNK))).isTrue();
        assertThat(spy.events()).containsExactly("pipeline 4", "load", "pipeline 4");
    }

    @Test
    void batchWithSomeCallsUnknownToTheServerLoadsOnceAndResendsOnlyThose() {
        store.enqueue(task(10).build());
        store.enqueue(task(20).build());
        spy.rejectInNextPipeline(call -> call % 2 == 1);

        var outcomes = spied.enqueueAll(batch(4 * RedisTaskStore.ENQUEUE_CHUNK));

        assertBatchOutcomes(outcomes, 4 * RedisTaskStore.ENQUEUE_CHUNK);
        assertThat(spy.events()).containsExactly("pipeline 4", "load", "pipeline 2");
    }

    @Test
    void largePayloadsSplitAnEnqueueBatchSoThatOneScriptStaysShort() {
        var big = "x".repeat(200 * 1024);

        var outcomes = spied.enqueueAll(List.of(
                task(1).encodedPayload(big).build(), task(2).encodedPayload(big).build(), task(3).build()));

        assertThat(outcomes).containsOnly(EnqueueOutcome.STORED).hasSize(3);
        assertThat(spy.events()).containsExactly("pipeline 2");
        assertThat(store.task(2).orElseThrow().encodedPayload()).isEqualTo(big);
    }

    @Test
    void aSecondEvictionAfterTheLoadFallsBackToOneCallAtATimeAndLoadsAgainOnce() {
        jedis.scriptFlush();
        spy.flushAfterLoads(1);

        var outcomes = spied.enqueueAll(batch(5 * RedisTaskStore.ENQUEUE_CHUNK));

        assertThat(outcomes).containsOnly(EnqueueOutcome.STORED).hasSize(5 * RedisTaskStore.ENQUEUE_CHUNK);
        assertThat(jedis.exists(keys.task(5 * RedisTaskStore.ENQUEUE_CHUNK))).isTrue();
        assertThat(spy.events())
                .containsExactly(
                        "pipeline 5",
                        "load",
                        "pipeline 5",
                        "evalsha",
                        "load",
                        "evalsha",
                        "evalsha",
                        "evalsha",
                        "evalsha",
                        "evalsha");
    }

    @Test
    void aScriptErrorThatIsNotNoScriptIsNotRetriedAndLoadsNothing() {
        var broken = store.queueId("broken");
        store.enqueue(task(99).build());
        jedis.set(keys.queue(broken, type), "not a sorted set");

        assertThatThrownBy(() -> spied.enqueueAll(
                        List.of(task(1).build(), task(2).queueId(broken).build(), task(3).build())))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("WRONGTYPE")
                .cause()
                .isInstanceOf(JedisDataException.class)
                .isNotInstanceOf(JedisNoScriptException.class);

        assertThat(spy.events()).containsExactly("pipeline 1");
        assertThat(jedis.exists(keys.task(1))).isTrue();
    }

    private List<LeaseRef> leaseRefs(int count, int wrongEvery) {
        var tasks = new ArrayList<TaskRecord>();
        for (var i = 1; i <= count; i++) {
            tasks.add(task(i).build());
        }
        store.enqueueAll(tasks);
        var refs = new ArrayList<LeaseRef>();
        for (var t : claim(count)) {
            refs.add(new LeaseRef(t.id(), t.revision() + (t.id() % wrongEvery == 0 ? 1 : 0)));
        }
        return refs;
    }

    @Test
    void renewAfterScriptFlushStillReturnsEveryStatusInOrder() {
        var a = claimOne(1);
        store.enqueue(task(2).build());
        var b = claim(1).getFirst();
        jedis.scriptFlush();

        var statuses = spied.renew(
                List.of(
                        new LeaseRef(2, b.revision()),
                        new LeaseRef(1, a.revision() + 1),
                        new LeaseRef(1, a.revision())),
                LEASE);

        assertThat(statuses).extracting(s -> s.id()).containsExactly(2L, 1L, 1L);
        assertThat(statuses).extracting(s -> s.owned()).containsExactly(true, false, true);
        assertThat(spy.events()).containsExactly("pipeline 1", "load", "pipeline 1");
    }

    @Test
    void renewWithOneChunkUnknownToTheServerLoadsOnceAndResendsOnlyThatChunk() {
        var refs = leaseRefs(600, 100);
        store.renew(refs.subList(0, 1), LEASE);
        spy.rejectInNextPipeline(call -> call == 1);

        var statuses = spied.renew(refs, LEASE);

        assertThat(statuses).hasSize(600);
        for (var i = 0; i < refs.size(); i++) {
            assertThat(statuses.get(i).id()).isEqualTo(refs.get(i).id());
            assertThat(statuses.get(i).owned())
                    .as("id %d", refs.get(i).id())
                    .isEqualTo(refs.get(i).id() % 100 != 0);
        }
        assertThat(spy.events()).containsExactly("pipeline 3", "load", "pipeline 1");
    }

    // ---- transport resend and lost replies ------------------------------------------------------------

    @Test
    void enqueueResentByTheTransportWithTheSameTokenIsStoredOnceAndReportedStored() {
        spy.resendEveryEvalsha(true);

        var outcome = spied.enqueue(task(1).uniqueKey("k").build());

        assertThat(outcome).isEqualTo(EnqueueOutcome.STORED);
        assertThat(jedis.zrange(keys.queue(queue, type), 0, -1)).containsExactly("1");
        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("1");
        assertThat(store.counts().get(queue).queued()).isEqualTo(1);
        spy.resendEveryEvalsha(false);
        assertThat(store.enqueue(task(1).build())).isEqualTo(EnqueueOutcome.EXISTS);
    }

    @Test
    void batchResentByTheTransportWithTheSameTokensIsStoredOnceAndReportedStored() {
        var tasks = List.of(
                task(1).uniqueKey("k").build(),
                task(2).build(),
                task(3).uniqueKey("j").build());
        spy.resendEveryEvalsha(true);

        var outcomes = spied.enqueueAll(tasks);

        assertThat(outcomes).containsExactly(EnqueueOutcome.STORED, EnqueueOutcome.STORED, EnqueueOutcome.STORED);
        assertThat(jedis.zrange(keys.queue(queue, type), 0, -1)).containsExactlyInAnyOrder("1", "2", "3");
        assertThat(store.counts().get(queue).queued()).isEqualTo(3);
        spy.resendEveryEvalsha(false);
        assertThat(store.enqueueAll(tasks)).containsOnly(EnqueueOutcome.EXISTS);
    }

    @Test
    void enqueueWhoseReplyWasLostIsAcceptedOnResendWithoutADuplicate() {
        var type = TaskType.of("lost-reply");
        var client = TaskClient.builder()
                .store(spied)
                .jsonMapper(new JsonMapperTest())
                .build();
        client.enqueue(TaskRequest.of(type).build());
        var id = client.nextId();
        var request = TaskRequest.of(type).id(id).uniqueKey("lost").build();
        spy.loseNextReply();

        assertThatThrownBy(() -> client.enqueue(request))
                .isInstanceOfSatisfying(
                        EnqueueFailedException.class, e -> assertThat(e.ids()).containsExactly(id));
        assertThat(jedis.exists(keys.task(id))).isTrue();

        assertThat(client.enqueue(request))
                .as("the first call stored the task, so the resend wrote nothing")
                .isEqualTo(new EnqueueResult.Accepted(id, false));
        assertThat(store.counts().get(store.queueId(Queues.GENERAL)).queued()).isEqualTo(2);
        assertThat(jedis.get(keys.uniqueKey("lost"))).isEqualTo(String.valueOf(id));
    }

    @Test
    void batchWhoseReplyWasLostIsAcceptedOnRetryWithoutDuplicates() {
        var type = TaskType.of("lost-reply");
        var client = TaskClient.builder()
                .store(spied)
                .jsonMapper(new JsonMapperTest())
                .build();
        client.enqueue(TaskRequest.of(type).build());
        var requests = List.of(
                TaskRequest.of(type).uniqueKey("a").build(),
                TaskRequest.of(type).build(),
                TaskRequest.of(type).uniqueKey("c").build());
        spy.loseNextReply();

        EnqueueFailedException failed = null;
        try {
            client.enqueueAll(requests);
        } catch (EnqueueFailedException e) {
            failed = e;
        }
        assertThat(failed).isNotNull();
        assertThat(failed.ids()).hasSize(3);
        assertThat(jedis.exists(keys.task(failed.ids().getFirst()))).isTrue();

        var results = client.enqueueAll(failed.retryRequests(requests));

        assertThat(results).hasSize(3).allSatisfy(r -> assertThat(r).isInstanceOf(EnqueueResult.Accepted.class));
        assertThat(store.counts().get(store.queueId(Queues.GENERAL)).queued()).isEqualTo(4);
    }

    // ---- promote -------------------------------------------------------------------------------------

    @Test
    void promoteWithManyEmptyQueuesMovesTheDueTaskAndReportsTheNextDue() {
        for (var i = 0; i < 200; i++) {
            store.queueId("empty" + i);
        }
        var later = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
        var laterQueue = store.queueId("later");
        store.enqueue(task(1).dueAt(Instant.now().plusSeconds(1800)).build());
        store.enqueue(task(2).queueId(laterQueue).dueAt(later).build());
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        RedisFixtures.ageDue(jedis, keys, queue, 1, Instant.now().minusSeconds(5));

        var first = store.promote(100);

        assertThat(first.moved()).isEqualTo(1);
        assertThat(first.nextDue()).isEqualTo(later);
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.task(2).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
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
        store.enqueue(task(1).dueAt(Instant.now().plusSeconds(1800)).build());
        store.enqueue(
                task(2).queueId(other).dueAt(Instant.now().plusSeconds(1800)).build());
        store.enqueue(task(3).dueAt(Instant.now().plusSeconds(1800)).build());
        var past = Instant.now().minusSeconds(5);
        RedisFixtures.ageDue(jedis, keys, queue, 1, past);
        RedisFixtures.ageDue(jedis, keys, other, 2, past);
        RedisFixtures.ageDue(jedis, keys, queue, 3, past);

        var r = store.promote(2);

        assertThat(r.moved()).isEqualTo(2);
        assertThat(r.nextDue()).isNotNull().isBeforeOrEqualTo(Instant.now());
        assertThat(store.promote(2).moved()).isEqualTo(1);
        assertThat(store.promote(2).nextDue()).isNull();
    }

    // ---- listing ---------------------------------------------------------------------------------------

    private List<Long> listedIds(TaskState state, Integer queueId, int limit) {
        var ids = new ArrayList<Long>();
        String cursor = null;
        do {
            var page = store.tasks(state, queueId, cursor, limit);
            page.items().forEach(t -> ids.add(t.id()));
            cursor = page.next();
        } while (cursor != null);
        return ids;
    }

    @Test
    void aListingPageOfQueuedTasksIsOneScriptCallForAnyNumberOfQueuesAndTypes() {
        for (var q = 0; q < 4; q++) {
            for (var t = 0; t < 5; t++) {
                store.enqueue(task(q * 10 + t + 1)
                        .queueId(store.queueId("q" + q))
                        .typeId(store.typeId("t" + t))
                        .build());
            }
        }
        store.tasks(TaskState.QUEUED, null, null, 5);

        var first = ServerCounters.measure(jedis, () -> store.tasks(TaskState.QUEUED, null, null, 5));
        var all = ServerCounters.measure(jedis, () -> store.tasks(TaskState.QUEUED, null, null, 100));
        var one = ServerCounters.measure(jedis, () -> store.tasks(TaskState.QUEUED, store.queueId("q1"), null, 5));

        assertThat(first.calls("evalsha")).isEqualTo(1);
        assertThat(all.calls("evalsha")).isEqualTo(1);
        assertThat(one.calls("evalsha")).isEqualTo(1);
        assertThat(first.roundTrips()).isEqualTo(2);
    }

    @Test
    void aListingPageOfTheOtherStatesIsOneScriptCallForAnyNumberOfQueues() {
        for (var q = 0; q < 4; q++) {
            store.enqueue(task(q + 1)
                    .queueId(store.queueId("q" + q))
                    .dueAt(Instant.now().plusSeconds(3600))
                    .build());
        }
        store.claim(new ClaimRequest(List.of(new QueueGrant(store.queueId("q0"), 1, 1)), 1, LEASE, 1, null));
        store.enqueue(task(10).queueId(store.queueId("q2")).build());
        var claimed =
                store.claim(new ClaimRequest(List.of(new QueueGrant(store.queueId("q2"), 1, 1)), 1, LEASE, 1, null));
        store.ack(claimed.getFirst().id(), claimed.getFirst().revision(), true, true, null);
        store.tasks(TaskState.SCHEDULED, null, null, 5);

        for (var state : List.of(TaskState.SCHEDULED, TaskState.LEASED, TaskState.SUCCEEDED)) {
            var counts = ServerCounters.measure(jedis, () -> store.tasks(state, null, null, 5));
            assertThat(counts.calls("evalsha")).as(state.name()).isEqualTo(1);
        }
    }

    @Test
    void aMergedListingKeepsScoreThenIdTextOrderAcrossQueuesTypesAndPages() {
        var queues = List.of(queue, store.queueId("b"), store.queueId("c"));
        var types = List.of(type, store.typeId("u"), store.typeId("v"), store.typeId("w"));
        var expected = new ArrayList<Long>();
        var due = new HashMap<Long, Long>();
        for (var id = 90L; id <= 130; id++) {
            var at = 1_700_000_000_000L + (id % 3) * 1000;
            store.enqueue(task(id).queueId(queues.get((int) (id % 3)))
                    .typeId(types.get((int) (id % 4)))
                    .dueAt(Instant.ofEpochMilli(at))
                    .build());
            due.put(id, at);
            expected.add(id);
        }
        expected.sort(java.util.Comparator.<Long>comparingLong(due::get).thenComparing(String::valueOf));

        assertThat(listedIds(TaskState.QUEUED, null, 7)).isEqualTo(expected);
        assertThat(listedIds(TaskState.QUEUED, null, 1)).isEqualTo(expected);
        assertThat(listedIds(TaskState.QUEUED, null, 100)).isEqualTo(expected);
        var inQueue = expected.stream().filter(id -> id % 3 == 1).toList();
        assertThat(listedIds(TaskState.QUEUED, queues.get(1), 4)).isEqualTo(inQueue);
    }

    @Test
    void aListingLongerThanOneChunkTakesOneScriptCallPerChunkAndKeepsTheOrder() {
        var other = store.queueId("other");
        var tasks = new ArrayList<TaskRecord>();
        for (var id = 1L; id <= 1100; id++) {
            tasks.add(task(id).queueId(id % 2 == 0 ? queue : other)
                    .typeId(store.typeId("t" + id % 3))
                    .dueAt(Instant.ofEpochMilli(1_700_000_000_000L + id % 5))
                    .build());
        }
        store.enqueueAll(tasks);
        var expected = tasks.stream()
                .sorted(java.util.Comparator.<TaskRecord>comparingLong(
                                t -> t.dueAt().toEpochMilli())
                        .thenComparing(t -> String.valueOf(t.id())))
                .map(TaskRecord::id)
                .toList();
        store.tasks(TaskState.QUEUED, null, null, 1);
        var page = new java.util.concurrent.atomic.AtomicReference<today.bonfire.oss.bth4j.spi.TaskPage>();

        var counts = ServerCounters.measure(jedis, () -> page.set(store.tasks(TaskState.QUEUED, null, null, 1100)));

        assertThat(page.get().items()).extracting(TaskRecord::id).isEqualTo(expected);
        assertThat(page.get().next()).isNull();
        assertThat(counts.calls("evalsha")).isEqualTo(3);
    }

    // ---- idle claim ------------------------------------------------------------------------------------

    private static final long HALF_TYPE_SET_TTL_MS = RedisScripts.TYPE_SET_TTL_MS / 2;

    private String typeSetAfterOneClaim(Set<Integer> known) {
        store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known));
        return jedis.keys(keys.prefix + "ts:*").iterator().next();
    }

    @Test
    void aClaimThatFindsNothingDoesNotWriteWhileTheTypeSetHasMoreThanHalfItsTimeLeft() {
        var known = Set.of(type, store.typeId("idle"));
        var set = typeSetAfterOneClaim(known);
        var left = jedis.pttl(set);

        var counts = ServerCounters.measure(jedis, () -> {
            for (var i = 0; i < 5; i++) {
                assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known)))
                        .isEmpty();
            }
        });

        assertThat(counts.calls("evalsha")).isEqualTo(5);
        assertThat(counts.calls("pexpire")).isZero();
        assertThat(jedis.pttl(set)).isLessThanOrEqualTo(left).isGreaterThan(HALF_TYPE_SET_TTL_MS);
    }

    @Test
    void aClaimExtendsTheTypeSetToTheWholeTimeOnceLessThanHalfIsLeft() {
        var known = Set.of(type, store.typeId("idle"));
        var set = typeSetAfterOneClaim(known);
        jedis.pexpire(set, HALF_TYPE_SET_TTL_MS - 5_000);

        store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known));

        assertThat(jedis.pttl(set))
                .isGreaterThan(RedisScripts.TYPE_SET_TTL_MS - 5_000)
                .isLessThanOrEqualTo(RedisScripts.TYPE_SET_TTL_MS);
    }

    @Test
    void aClaimLeavesTheTypeSetAtHalfItsTimeAloneAndNeverLetsItRunOutWhileItKeepsClaiming() {
        var known = Set.of(type, store.typeId("idle"));
        var set = typeSetAfterOneClaim(known);
        jedis.pexpire(set, HALF_TYPE_SET_TTL_MS + 60_000);

        store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known));

        assertThat(jedis.pttl(set))
                .isGreaterThan(HALF_TYPE_SET_TTL_MS)
                .isLessThanOrEqualTo(HALF_TYPE_SET_TTL_MS + 60_000);
        for (var step = 0; step < 4; step++) {
            jedis.pexpire(set, HALF_TYPE_SET_TTL_MS - 1);
            store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known));
            assertThat(jedis.pttl(set)).isGreaterThan(HALF_TYPE_SET_TTL_MS);
        }
    }

    @Test
    void aTypeSetNoClaimTouchesAnymoreKeepsItsExpiryAndIsGoneWhenItEnds() {
        var known = Set.of(type, store.typeId("idle"));
        var set = typeSetAfterOneClaim(known);

        assertThat(jedis.pttl(set)).isPositive().isLessThanOrEqualTo(RedisScripts.TYPE_SET_TTL_MS);
        jedis.pexpireAt(set, 1);

        assertThat(jedis.exists(set)).isFalse();
        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known)))
                .isEmpty();
        assertThat(jedis.pttl(set)).isPositive();
    }
}
