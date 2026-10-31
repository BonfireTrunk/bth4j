package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/**
 * The wave 2 operations of {@link RedisTaskStore} (cancel, expiry, admin, workers, schedule controls) against a
 * real Redis, including the checks that read Redis keys directly. Needs Docker.
 */
@Testcontainers
class RedisWave2IT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final AtomicInteger NAMESPACES = new AtomicInteger();

    private static RedisClient jedis;

    private RedisTaskStore store;
    private Keys keys;
    private int queue;
    private int event;

    @BeforeAll
    static void connect() {
        var poolConfig = ConnectionPoolConfig.builder()
                .maxPoolSize(16)
                .minPoolSize(1)
                .waitingForObjectTimeout(Duration.ofSeconds(5))
                .build();
        var clientConfig = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(3000)
                .clientName("bth4j-wave2-test")
                .build();
        jedis = RedisClient.builder()
                .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                .clientConfig(clientConfig)
                .poolConfig(poolConfig)
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
        store = new RedisTaskStore(jedis, "W" + NAMESPACES.incrementAndGet());
        keys = store.keys();
        store.init();
        queue = store.queueId("default");
        event = store.eventId("send");
    }

    private TaskRecord.Builder task(long id) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queue)
                .eventId(event)
                .maxAttempts(3)
                .created(Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }

    private TaskRecord enqueued(TaskRecord.Builder b) {
        var t = b.build();
        assertThat(store.enqueue(t)).isEqualTo(EnqueueOutcome.STORED);
        return store.getTask(t.id()).orElseThrow();
    }

    private TaskRecord enqueued(long id) {
        return enqueued(task(id));
    }

    /** Claims the only queued task, so call it before enqueueing others. */
    private TaskRecord leased(TaskRecord.Builder b) {
        enqueued(b);
        return store.claim(new ClaimRequest(List.of(queue), 1, LEASE, 1, null)).getFirst();
    }

    private TaskRecord leased(long id) {
        return leased(task(id));
    }

    private StateCounts counts() {
        return store.counts().getOrDefault(queue, StateCounts.ZERO);
    }

    private static Instant past() {
        return Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
    }

    // ---- finishCancelled -----------------------------------------------------------------------------

    @Test
    void finishCancelledMovesLeasedToCancelledAndDropsData() {
        enqueued(task(1).data("payload").account("a"));
        var t = store.claim(new ClaimRequest(List.of(queue), 1, LEASE, 7, null)).getFirst();

        assertThat(store.finishCancelled(1, t.revision(), "stop", false)).isTrue();

        var done = store.getTask(1).orElseThrow();
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.cancelReason()).isEqualTo("stop");
        assertThat(done.cancelRequestedAt()).isNotNull();
        assertThat(done.workerId()).isEqualTo(7);
        assertThat(done.leaseUntil()).isNull();
        assertThat(done.data()).isNull();
        assertThat(jedis.zscore(keys.leased, "1")).isNull();
        assertThat(jedis.zscore(keys.index(5, queue), "1")).isNotNull();
        assertThat(jedis.zscore(keys.index(2, queue), "1")).isNull();
        assertThat(counts().leased()).isZero();
        assertThat(counts().cancelled()).isEqualTo(1);
    }

    @Test
    void finishCancelledKeepsPayloadAndTheEarlierRequestTime() {
        enqueued(task(1).data("payload"));
        var t = store.claim(new ClaimRequest(List.of(queue), 1, LEASE, 1, null)).getFirst();
        assertThat(store.cancel(1, t.revision(), "ops")).isEqualTo(CancelOutcome.REQUESTED);
        var requested = store.getTask(1).orElseThrow().cancelRequestedAt();

        assertThat(store.finishCancelled(1, t.revision(), "ops", true)).isTrue();

        var done = store.getTask(1).orElseThrow();
        assertThat(done.data()).isEqualTo("payload");
        assertThat(done.cancelRequestedAt()).isEqualTo(requested);
    }

    @Test
    void finishCancelledRefusesAStaleRevisionAndANonLeasedTask() {
        var t = leased(1);
        var queued = enqueued(2);

        assertThat(store.finishCancelled(1, t.revision() + 1, "x", false)).isFalse();
        assertThat(store.finishCancelled(2, queued.revision(), "x", false)).isFalse();

        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void finishCancelledReleasesTheUniqueKeyOrStartsItsWindow() {
        enqueued(task(1).uniqueKey("a"));
        enqueued(task(2).uniqueKey("b").keyKeep(Duration.ofMinutes(5)));
        var claimed = store.claim(new ClaimRequest(List.of(queue), 2, LEASE, 1, null));

        for (var t : claimed) {
            assertThat(store.finishCancelled(t.id(), t.revision(), "x", false)).isTrue();
        }

        assertThat(jedis.exists(keys.uniqueKey("a"))).isFalse();
        assertThat(store.getTask(1).orElseThrow().uniqueKey()).isNull();
        assertThat(jedis.pttl(keys.uniqueKey("b"))).isPositive();
        assertThat(jedis.zscore(keys.keyWindows, "2")).isNotNull();
        assertThat(store.getTask(2).orElseThrow().keyUntil()).isNotNull();
    }

    // ---- expire and startBy --------------------------------------------------------------------------

    @Test
    void enqueueTracksStartByAndExpireMovesQueuedAndScheduledTasks() {
        enqueued(task(1).startBy(past()));
        enqueued(task(2).startBy(past()).due(Instant.now().plusSeconds(600)));
        enqueued(task(3).startBy(Instant.now().plusSeconds(600)));
        enqueued(4);
        assertThat(jedis.zcard(keys.startBy)).isEqualTo(3);

        var moved = store.expire(10);

        assertThat(moved).containsExactlyInAnyOrder(1L, 2L);
        var t = store.getTask(1).orElseThrow();
        assertThat(t.state()).isEqualTo(TaskState.EXPIRED);
        assertThat(t.revision()).isEqualTo(1);
        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.EXPIRED);
        assertThat(store.getTask(3).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(jedis.zrange(keys.startBy, 0, -1)).containsExactly("3");
        assertThat(jedis.zscore(keys.index(6, queue), "1")).isNotNull();
        assertThat(jedis.zscore(keys.queue(queue), "1")).isNull();
        assertThat(jedis.zscore(keys.scheduled, "2")).isNull();
        var c = counts();
        assertThat(c.queued()).isEqualTo(2);
        assertThat(c.scheduled()).isZero();
        assertThat(c.expired()).isEqualTo(2);
        assertThat(store.expire(10)).isEmpty();
    }

    @Test
    void expireHonoursTheLimitAndHandlesKeysLikeATerminalTransition() {
        enqueued(task(1).startBy(past()).uniqueKey("k"));
        enqueued(task(2).startBy(past()).uniqueKey("w").keyKeep(Duration.ofMinutes(5)));
        enqueued(task(3).startBy(past()));

        assertThat(store.expire(2)).hasSize(2);
        assertThat(store.expire(2)).hasSize(1);

        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
        assertThat(jedis.pttl(keys.uniqueKey("w"))).isPositive();
        assertThat(jedis.zscore(keys.keyWindows, "2")).isNotNull();
    }

    @Test
    void startByIsKeptAcrossPromoteAndDroppedOnClaim() throws InterruptedException {
        enqueued(task(1).startBy(Instant.now().plusSeconds(600))
                .due(Instant.now().plusMillis(200)));
        assertThat(jedis.zscore(keys.startBy, "1")).isNotNull();
        Thread.sleep(400);
        assertThat(store.promote(10).moved()).isEqualTo(1);
        assertThat(jedis.zscore(keys.startBy, "1")).isNotNull();

        var t = store.claim(new ClaimRequest(List.of(queue), 1, LEASE, 1, null)).getFirst();

        assertThat(t.id()).isEqualTo(1);
        assertThat(jedis.zscore(keys.startBy, "1")).isNull();
    }

    @Test
    void claimExpiresAStartByPassedTaskInlineAndContinues() {
        enqueued(task(1).startBy(past()).uniqueKey("k"));
        enqueued(2);

        var claimed = store.claim(new ClaimRequest(List.of(queue), 5, LEASE, 1, null));

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(2L);
        var expired = store.getTask(1).orElseThrow();
        assertThat(expired.state()).isEqualTo(TaskState.EXPIRED);
        assertThat(expired.revision()).isEqualTo(1);
        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
        assertThat(jedis.zscore(keys.startBy, "1")).isNull();
        var c = counts();
        assertThat(c.queued()).isZero();
        assertThat(c.leased()).isEqualTo(1);
        assertThat(c.expired()).isEqualTo(1);
    }

    // ---- paused queues -------------------------------------------------------------------------------

    @Test
    void pausedQueuesAreSkippedByClaimUntilResumed() {
        var other = store.queueId("other");
        enqueued(1);
        enqueued(task(2).queueId(other));

        store.pauseQueue(queue, true);
        var whilePaused = store.claim(new ClaimRequest(List.of(queue, other), 5, LEASE, 1, null));
        store.pauseQueue(queue, false);
        var resumed = store.claim(new ClaimRequest(List.of(queue, other), 5, LEASE, 1, null));

        assertThat(whilePaused).extracting(TaskRecord::id).containsExactly(2L);
        assertThat(resumed).extracting(TaskRecord::id).containsExactly(1L);
    }

    @Test
    void queuesListPausedFlagsInIdOrder() {
        var other = store.queueId("other");
        store.pauseQueue(other, true);

        assertThat(store.queues())
                .containsExactly(new QueueInfo(queue, "default", false), new QueueInfo(other, "other", true));

        store.pauseQueue(other, false);
        store.pauseQueue(other, false);

        assertThat(store.queues()).extracting(QueueInfo::paused).containsOnly(false);
    }

    // ---- admin cancel, retry, delete -----------------------------------------------------------------

    @Test
    void cancelMovesQueuedAndScheduledTasksAtOnce() {
        var q = enqueued(task(1).startBy(Instant.now().plusSeconds(600)).uniqueKey("k"));
        var s = enqueued(task(2).due(Instant.now().plusSeconds(600)));

        assertThat(store.cancel(1, q.revision(), "no")).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(store.cancel(2, s.revision(), null)).isEqualTo(CancelOutcome.CANCELLED);

        var a = store.getTask(1).orElseThrow();
        assertThat(a.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(a.revision()).isEqualTo(q.revision() + 1);
        assertThat(a.cancelReason()).isEqualTo("no");
        assertThat(a.cancelRequestedAt()).isNotNull();
        var b = store.getTask(2).orElseThrow();
        assertThat(b.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(b.cancelReason()).isNull();
        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
        assertThat(jedis.zcard(keys.startBy)).isZero();
        assertThat(jedis.zcard(keys.scheduled)).isZero();
        var c = counts();
        assertThat(c.queued()).isZero();
        assertThat(c.scheduled()).isZero();
        assertThat(c.cancelled()).isEqualTo(2);
    }

    @Test
    void cancelOfALeasedTaskOnlyStoresTheRequest() {
        var t = leased(1);

        assertThat(store.cancel(1, t.revision(), "why")).isEqualTo(CancelOutcome.REQUESTED);

        var after = store.getTask(1).orElseThrow();
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
        assertThat(after.revision()).isEqualTo(t.revision());
        assertThat(after.cancelReason()).isEqualTo("why");
        assertThat(after.cancelRequestedAt()).isNotNull();
        assertThat(store.renew(List.of(new LeaseRef(1, t.revision())), LEASE)
                        .getFirst()
                        .cancelRequested())
                .isTrue();
    }

    @Test
    void cancelIsStaleOnRevisionMismatchTerminalStateOrUnknownId() {
        var done = leased(2);
        store.ack(2, done.revision(), true, false);
        var t = enqueued(1);

        assertThat(store.cancel(1, t.revision() + 1, "x")).isEqualTo(CancelOutcome.STALE);
        assertThat(store.cancel(2, store.getTask(2).orElseThrow().revision(), "x"))
                .isEqualTo(CancelOutcome.STALE);
        assertThat(store.cancel(99, 0, "x")).isEqualTo(CancelOutcome.STALE);

        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void retryReturnsTerminalTasksToTheQueue() {
        var failure = new FailureInfo(1, Instant.now().truncatedTo(ChronoUnit.MILLIS), "E", "m", null);
        var dead = leased(1);
        assertThat(store.fail(1, dead.revision(), failure, null, true)).isEqualTo(FailOutcome.DEAD);
        var cancelled = enqueued(task(2).data("d"));
        assertThat(store.cancel(2, cancelled.revision(), "r")).isEqualTo(CancelOutcome.CANCELLED);
        enqueued(task(3).startBy(past()));
        store.expire(10);

        for (var id : List.of(1L, 2L, 3L)) {
            var before = store.getTask(id).orElseThrow();
            assertThat(store.retry(id, before.revision())).isTrue();
            var after = store.getTask(id).orElseThrow();
            assertThat(after.state()).isEqualTo(TaskState.QUEUED);
            assertThat(after.revision()).isEqualTo(before.revision() + 1);
            assertThat(after.attempts()).isZero();
            assertThat(after.cancelReason()).isNull();
            assertThat(after.cancelRequestedAt()).isNull();
            assertThat(after.workerId()).isNull();
            assertThat(after.leaseUntil()).isNull();
            assertThat(jedis.zscore(keys.queue(queue), String.valueOf(id))).isNotNull();
        }
        assertThat(store.getTask(1).orElseThrow().failures()).hasSize(1);
        assertThat(store.getTask(2).orElseThrow().data()).isEqualTo("d");
        var c = counts();
        assertThat(c.queued()).isEqualTo(3);
        assertThat(c.dead() + c.cancelled() + c.expired()).isZero();
        assertThat(jedis.zcard(keys.index(4, queue))
                        + jedis.zcard(keys.index(5, queue))
                        + jedis.zcard(keys.index(6, queue)))
                .isZero();
    }

    @Test
    void retryClearsTheKeyFieldsAndReleasesTheWindowedKey() {
        var t = enqueued(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)));
        assertThat(store.cancel(1, t.revision(), "x")).isEqualTo(CancelOutcome.CANCELLED);
        var cancelled = store.getTask(1).orElseThrow();
        assertThat(cancelled.keyUntil()).isNotNull();

        assertThat(store.retry(1, cancelled.revision())).isTrue();

        var after = store.getTask(1).orElseThrow();
        assertThat(after.uniqueKey()).isNull();
        assertThat(after.keyKeep()).isNull();
        assertThat(after.keyUntil()).isNull();
        assertThat(jedis.zscore(keys.keyWindows, "1")).isNull();
        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
    }

    @Test
    void retryRefusesAWrongRevisionAndNonTerminalStates() {
        var t = leased(2);
        assertThat(store.finishCancelled(2, t.revision(), "x", true)).isTrue();
        var cancelled = store.getTask(2).orElseThrow();
        var queued = enqueued(1);

        assertThat(store.retry(1, queued.revision())).isFalse();
        assertThat(store.retry(2, cancelled.revision() - 1)).isFalse();
        assertThat(store.retry(99, 0)).isFalse();

        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.CANCELLED);
        assertThat(counts().cancelled()).isEqualTo(1);
    }

    @Test
    void deleteRemovesATaskFromWhereverItIs() {
        var done = leased(3);
        store.ack(3, done.revision(), true, true);
        var ended = leased(task(4).uniqueKey("w").keyKeep(Duration.ofMinutes(5)));
        store.ack(4, ended.revision(), true, true);
        var queued = enqueued(task(1).startBy(Instant.now().plusSeconds(600)).uniqueKey("k"));
        var scheduled = enqueued(task(2).due(Instant.now().plusSeconds(600)));

        assertThat(store.delete(1, queued.revision())).isTrue();
        assertThat(store.delete(2, scheduled.revision())).isTrue();
        assertThat(store.delete(3, store.getTask(3).orElseThrow().revision())).isTrue();
        assertThat(store.delete(4, store.getTask(4).orElseThrow().revision())).isTrue();

        for (var id = 1; id <= 4; id++) {
            assertThat(jedis.exists(keys.task(id))).isFalse();
        }
        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
        assertThat(jedis.exists(keys.uniqueKey("w"))).isFalse();
        assertThat(jedis.zcard(keys.startBy)).isZero();
        assertThat(jedis.zcard(keys.keyWindows)).isZero();
        assertThat(jedis.zcard(keys.index(3, queue))).isZero();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
    }

    @Test
    void deleteRefusesALeasedTaskAndAWrongRevision() {
        var t = leased(1);
        var q = enqueued(2);

        assertThat(store.delete(1, t.revision())).isFalse();
        assertThat(store.delete(2, q.revision() + 1)).isFalse();
        assertThat(store.delete(99, 0)).isFalse();

        assertThat(store.getTask(1)).isPresent();
        assertThat(store.getTask(2)).isPresent();
        assertThat(counts().leased()).isEqualTo(1);
        assertThat(counts().queued()).isEqualTo(1);
    }

    // ---- workers -------------------------------------------------------------------------------------

    @Test
    void workersAreListedInIdOrderWithHeartbeats() {
        var a = store.allocateWorkerId("a");
        var b = store.allocateWorkerId("b");
        assertThat(store.workers()).extracting(WorkerInfo::running).containsOnly(0);
        var registered = store.workers().getFirst();
        assertThat(registered.startedAt()).isNotNull();
        assertThat(registered.lastSeen()).isNotNull();

        store.heartbeat(b, 4);

        var workers = store.workers();
        assertThat(workers).extracting(WorkerInfo::id).containsExactly(a, b);
        assertThat(workers).extracting(WorkerInfo::label).containsExactly("a", "b");
        assertThat(workers.get(1).running()).isEqualTo(4);
        assertThat(workers.get(1).lastSeen()).isAfterOrEqualTo(workers.get(1).startedAt());
        assertThat(jedis.zscore(keys.workers, String.valueOf(b))).isNotNull();
    }

    @Test
    void purgeWorkersRemovesOnlyStaleOnes() {
        var stale = store.allocateWorkerId("stale");
        var fresh = store.allocateWorkerId("fresh");
        jedis.zadd(keys.workers, Instant.now().minusSeconds(3600).toEpochMilli(), String.valueOf(stale));

        assertThat(store.purgeWorkers(Duration.ofMinutes(10))).isEqualTo(1);

        assertThat(store.workers()).extracting(WorkerInfo::id).containsExactly(fresh);
        assertThat(jedis.exists(keys.worker(stale))).isFalse();
        assertThat(store.purgeWorkers(Duration.ofMinutes(10))).isZero();
        store.heartbeat(stale, 1);
        assertThat(store.workers()).extracting(WorkerInfo::id).containsExactly(fresh);
    }

    // ---- recurring -----------------------------------------------------------------------------------

    private RecurringDefinition cron(String id) {
        return new RecurringDefinition(id, event, 2, queue, "acme", "* * * * *", "{\"d\":1}", 5, 0);
    }

    @Test
    void recurringDefinitionsRoundTripEveryField() {
        var fire = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        var full = new RecurringDefinition(
                "full",
                event,
                3,
                queue,
                "acme",
                "0 9 * * *",
                "Europe/Berlin",
                "CATCH_UP",
                4,
                null,
                true,
                "d",
                6,
                false,
                0);
        var delay = new RecurringDefinition(
                "delay", event, 1, queue, null, null, null, null, 0, 30_000L, false, null, 1, true, 0);

        store.putRecurring(delay, fire.plusSeconds(1));
        store.putRecurring(full, fire);

        var listed = store.recurringDefinitions();
        assertThat(listed).extracting(r -> r.definition().scheduleId()).containsExactly("delay", "full");
        assertThat(listed.get(1).nextFire()).isEqualTo(fire);
        assertThat(listed.get(1).definition().withRevision(0)).isEqualTo(full);
        assertThat(listed.get(1).definition().revision()).isPositive();
        assertThat(listed.get(0).definition().withRevision(0)).isEqualTo(delay);
        assertThat(listed.get(0).nextFire()).isEqualTo(fire.plusSeconds(1));
        assertThat(jedis.hkeys(keys.cronDefinition("delay")))
                .doesNotContain("cron", "zone", "mf", "cum", "os", "acc", "data");
        assertThat(store.recurringSchedules()).containsOnlyKeys("delay", "full");
    }

    @Test
    void pausedDefinitionsAreNotDueAndKeepTheirFireTime() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        store.putRecurring(cron("s"), fire);

        assertThat(store.pauseRecurring("s", true)).isTrue();

        assertThat(store.dueRecurring(Instant.now(), 10)).isEmpty();
        assertThat(jedis.zscore(keys.cron, "s")).isNull();
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();
        assertThat(store.recurringDefinitions().getFirst().nextFire()).isEqualTo(fire);

        assertThat(store.pauseRecurring("s", false)).isTrue();

        var due = store.dueRecurring(Instant.now(), 10);
        assertThat(due).hasSize(1);
        assertThat(due.getFirst().fireAt()).isEqualTo(fire);
        assertThat(due.getFirst().definition().paused()).isFalse();
        assertThat(store.pauseRecurring("missing", true)).isFalse();
    }

    @Test
    void putTakesThePausedFlagOfTheNewDefinitionAndKeepsTheFireTimeWhileTheCronIsUnchanged() {
        var fire = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        store.putRecurring(cron("s"), fire);
        store.putRecurring(cron("s").withPaused(true), fire.plusSeconds(600));

        assertThat(jedis.zscore(keys.cron, "s")).isNull();
        assertThat(store.recurringDefinitions().getFirst().nextFire()).isEqualTo(fire);
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();

        var zoned = new RecurringDefinition(
                "s", event, 2, queue, "acme", "* * * * *", "Asia/Kolkata", null, 0, null, false, null, 5, false, 0);
        store.putRecurring(zoned, fire.plusSeconds(600));

        assertThat(store.recurringSchedules()).containsEntry("s", fire);
        assertThat(jedis.zscore(keys.cron, "s")).isNotNull();

        var changed = new RecurringDefinition(
                "s", event, 2, queue, "acme", "0 * * * *", null, null, 0, null, false, null, 5, false, 0);
        store.putRecurring(changed, fire.plusSeconds(600));

        assertThat(store.recurringSchedules()).containsEntry("s", fire.plusSeconds(600));
    }

    @Test
    void setNextFireReArmsAndRefusesUnknownDefinitions() {
        var fire = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
        store.putRecurring(cron("s"), fire);
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        assertThat(store.setNextFire("s", now)).isTrue();

        assertThat(store.recurringSchedules()).containsEntry("s", now);
        assertThat(store.dueRecurring(now.plusSeconds(1), 10)).hasSize(1);
        assertThat(store.setNextFire("missing", now)).isFalse();

        store.pauseRecurring("s", true);
        assertThat(store.setNextFire("s", fire)).isTrue();
        assertThat(store.dueRecurring(fire.plusSeconds(1), 10)).isEmpty();
        store.pauseRecurring("s", false);
        assertThat(store.recurringSchedules()).containsEntry("s", fire);
    }

    @Test
    void deleteRecurringRemovesItFromTheListing() {
        store.putRecurring(cron("s"), Instant.now().plusSeconds(60));

        assertThat(store.deleteRecurring("s")).isTrue();

        assertThat(store.recurringDefinitions()).isEmpty();
        assertThat(jedis.scard(keys.cronDefinitions)).isZero();
    }

    @Test
    void commitWithAHeldUniqueKeyAdvancesTheDefinitionWithoutStoringTheOccurrence() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        var overlap = new RecurringDefinition(
                "s", event, 2, queue, null, "* * * * *", null, null, 0, null, true, null, 5, false, 0);
        store.putRecurring(overlap, fire);
        var first = store.dueRecurring(Instant.now(), 10).getFirst();
        var next1 = fire.plusSeconds(60);
        var next2 = fire.plusSeconds(120);

        assertThat(store.commitRecurring(
                        first, next1, task(70).uniqueKey("sched:s").build()))
                .isTrue();
        var second = new DueSchedule("s", next1, first.definition());
        assertThat(store.commitRecurring(
                        second, next2, task(71).uniqueKey("sched:s").build()))
                .isTrue();

        assertThat(store.getTask(70)).isPresent();
        assertThat(store.getTask(71)).isEmpty();
        assertThat(jedis.zscore(keys.scheduled, "71")).isNull();
        assertThat(jedis.get(keys.uniqueKey("sched:s"))).isEqualTo("70");
        assertThat(counts().scheduled()).isEqualTo(1);
        assertThat(store.recurringSchedules()).containsEntry("s", next2);
    }
}
