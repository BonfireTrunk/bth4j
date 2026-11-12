package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import today.bonfire.oss.bth4j.CancelOutcome;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.QueueInfo;
import today.bonfire.oss.bth4j.RetryOutcome;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.ExpiredTask;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.RunnerRecord;
import today.bonfire.oss.bth4j.spi.StoredSchedule;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * The wave 2 operations of {@link RedisTaskStore} (cancel, expiry, admin, runners, schedule controls) against a
 * real Redis, including the checks that read Redis keys directly. Needs Docker.
 */
@Testcontainers
class RedisWave2IT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(RedisImage.NAME)
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final AtomicInteger NAMESPACES = new AtomicInteger();

    private static RedisClient jedis;

    private RedisTaskStore store;
    private Keys keys;
    private int queue;
    private int type;

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
        type = store.typeId("send");
    }

    private TaskRecord.Builder task(long id) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queue)
                .typeId(type)
                .maxAttempts(3)
                .createdAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
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
        return store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, null))
                .getFirst();
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

    private Set<Integer> queuedTypes() {
        return store.queuedByType().getOrDefault(queue, Map.of()).keySet();
    }

    @Test
    void finishCancelledMovesLeasedToCancelledAndDropsData() {
        enqueued(task(1).encodedPayload("payload").account("a"));
        var t = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 7, null))
                .getFirst();

        assertThat(store.finishCancelled(1, t.revision(), "stop", false, null)).isTrue();

        var done = store.getTask(1).orElseThrow();
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.cancelReason()).isEqualTo("stop");
        assertThat(done.cancelRequestedAt()).isNotNull();
        assertThat(done.runnerId()).isEqualTo(7);
        assertThat(done.leaseUntil()).isNull();
        assertThat(done.encodedPayload()).isNull();
        assertThat(jedis.zscore(keys.leased, "1")).isNull();
        assertThat(jedis.zscore(keys.index(5, queue), "1")).isNotNull();
        assertThat(jedis.zscore(keys.index(2, queue), "1")).isNull();
        assertThat(counts().leased()).isZero();
        assertThat(counts().cancelled()).isEqualTo(1);
    }

    @Test
    void finishCancelledKeepsPayloadAndTheEarlierRequestTime() {
        enqueued(task(1).encodedPayload("payload"));
        var t = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, null))
                .getFirst();
        assertThat(store.cancel(1, t.revision(), "ops", null)).isEqualTo(CancelOutcome.REQUESTED);
        var requested = store.getTask(1).orElseThrow().cancelRequestedAt();

        assertThat(store.finishCancelled(1, t.revision(), "ops", true, null)).isTrue();

        var done = store.getTask(1).orElseThrow();
        assertThat(done.encodedPayload()).isEqualTo("payload");
        assertThat(done.cancelRequestedAt()).isEqualTo(requested);
    }

    @Test
    void finishCancelledRefusesAStaleRevisionAndANonLeasedTask() {
        var t = leased(1);
        var queued = enqueued(2);

        assertThat(store.finishCancelled(1, t.revision() + 1, "x", false, null)).isFalse();
        assertThat(store.finishCancelled(2, queued.revision(), "x", false, null))
                .isFalse();

        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void finishCancelledReleasesTheUniqueKeyOrStartsItsWindow() {
        enqueued(task(1).uniqueKey("a"));
        enqueued(task(2).uniqueKey("b").keyKeep(Duration.ofMinutes(5)));
        var claimed = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 2, 2)), 2, LEASE, 1, null));

        for (var t : claimed) {
            assertThat(store.finishCancelled(t.id(), t.revision(), "x", false, null))
                    .isTrue();
        }

        assertThat(jedis.exists(keys.uniqueKey("a"))).isFalse();
        assertThat(store.getTask(1).orElseThrow().uniqueKey()).isNull();
        assertThat(jedis.pttl(keys.uniqueKey("b"))).isPositive();
        assertThat(jedis.hpttl(keys.task(2), "ku").getFirst()).isPositive();
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

        assertThat(moved).extracting(ExpiredTask::id).containsExactlyInAnyOrder(1L, 2L);
        var t = store.getTask(1).orElseThrow();
        assertThat(t.state()).isEqualTo(TaskState.EXPIRED);
        assertThat(t.revision()).isEqualTo(1);
        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.EXPIRED);
        assertThat(store.getTask(3).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(jedis.zrange(keys.startBy, 0, -1)).containsExactly("3");
        assertThat(jedis.zscore(keys.index(6, queue), "1")).isNotNull();
        assertThat(jedis.zscore(keys.queue(queue, type), "1")).isNull();
        assertThat(jedis.zscore(keys.scheduled(queue), "2")).isNull();
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
        assertThat(jedis.hpttl(keys.task(2), "ku").getFirst()).isPositive();
    }

    @Test
    void startByIsKeptAcrossPromoteAndDroppedOnClaim() throws InterruptedException {
        enqueued(task(1).startBy(Instant.now().plusSeconds(600))
                .due(Instant.now().plusMillis(200)));
        assertThat(jedis.zscore(keys.startBy, "1")).isNotNull();
        Thread.sleep(400);
        assertThat(store.promote(10).moved()).isEqualTo(1);
        assertThat(jedis.zscore(keys.startBy, "1")).isNotNull();

        var t = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, null))
                .getFirst();

        assertThat(t.id()).isEqualTo(1);
        assertThat(jedis.zscore(keys.startBy, "1")).isNull();
    }

    @Test
    void claimLeavesAStartByPassedTaskForMaintenanceAndContinues() {
        var late = enqueued(task(1).startBy(past()).uniqueKey("k"));
        enqueued(2);

        var claimed = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, 1, null));

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(2L);
        assertThat(store.getTask(1)).contains(late);
        assertThat(jedis.zscore(keys.startBy, "1")).isNotNull();
        var c = counts();
        assertThat(c.queued()).isEqualTo(1);
        assertThat(c.leased()).isEqualTo(1);
        assertThat(c.expired()).isZero();

        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(1L);
        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.EXPIRED);
        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
    }

    // ---- paused queues -------------------------------------------------------------------------------

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

        assertThat(store.cancel(1, q.revision(), "no", null)).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(store.cancel(2, s.revision(), null, null)).isEqualTo(CancelOutcome.CANCELLED);

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
        assertThat(jedis.zcard(keys.scheduled(queue))).isZero();
        var c = counts();
        assertThat(c.queued()).isZero();
        assertThat(c.scheduled()).isZero();
        assertThat(c.cancelled()).isEqualTo(2);
    }

    @Test
    void cancelOfALeasedTaskOnlyStoresTheRequest() {
        var t = leased(1);

        assertThat(store.cancel(1, t.revision(), "why", null)).isEqualTo(CancelOutcome.REQUESTED);

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
    void cancelAnswersStaleForAnotherRevisionFinishedForAFinalStateAndNotFoundForAnUnknownId() {
        var done = leased(2);
        store.ack(2, done.revision(), true, false, null);
        var t = enqueued(1);

        assertThat(store.cancel(1, t.revision() + 1, "x", null)).isEqualTo(CancelOutcome.STALE);
        assertThat(store.cancel(2, store.getTask(2).orElseThrow().revision(), "x", null))
                .isEqualTo(CancelOutcome.FINISHED);
        assertThat(store.cancel(99, 0, "x", null)).isEqualTo(CancelOutcome.NOT_FOUND);

        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void retryReturnsTerminalTasksToTheQueue() {
        var failure = new FailureInfo(1, Instant.now().truncatedTo(ChronoUnit.MILLIS), "E", "m", null);
        var dead = leased(1);
        assertThat(store.fail(1, dead.revision(), failure, null, true, null)).isEqualTo(FailOutcome.DEAD);
        var cancelled = enqueued(task(2).encodedPayload("d"));
        assertThat(store.cancel(2, cancelled.revision(), "r", null)).isEqualTo(CancelOutcome.CANCELLED);
        enqueued(task(3).startBy(past()));
        store.expire(10);

        for (var id : List.of(1L, 2L, 3L)) {
            var before = store.getTask(id).orElseThrow();
            assertThat(store.retry(id, before.revision())).isEqualTo(RetryOutcome.RETRIED);
            var after = store.getTask(id).orElseThrow();
            assertThat(after.state()).isEqualTo(TaskState.QUEUED);
            assertThat(after.revision()).isEqualTo(before.revision() + 1);
            assertThat(after.attempts()).isZero();
            assertThat(after.cancelReason()).isNull();
            assertThat(after.cancelRequestedAt()).isNull();
            assertThat(after.runnerId()).isNull();
            assertThat(after.leaseUntil()).isNull();
            assertThat(jedis.zscore(keys.queue(queue, type), String.valueOf(id)))
                    .isNotNull();
        }
        assertThat(store.getTask(1).orElseThrow().failures()).hasSize(1);
        assertThat(store.getTask(2).orElseThrow().encodedPayload()).isEqualTo("d");
        var c = counts();
        assertThat(c.queued()).isEqualTo(3);
        assertThat(c.dead() + c.cancelled() + c.expired()).isZero();
        assertThat(jedis.zcard(keys.index(4, queue))
                        + jedis.zcard(keys.index(5, queue))
                        + jedis.zcard(keys.index(6, queue)))
                .isZero();
    }

    @Test
    void retryKeepsTheKeyFieldsAndTakesTheWindowedKeyAgainWithoutExpiry() {
        var t = enqueued(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)));
        assertThat(store.cancel(1, t.revision(), "x", null)).isEqualTo(CancelOutcome.CANCELLED);
        var cancelled = store.getTask(1).orElseThrow();
        assertThat(cancelled.keyUntil()).isNotNull();
        assertThat(jedis.zscore(keys.index(5, queue), "1")).isNotNull();

        assertThat(store.retry(1, cancelled.revision())).isEqualTo(RetryOutcome.RETRIED);

        var after = store.getTask(1).orElseThrow();
        assertThat(after.uniqueKey()).isEqualTo("k");
        assertThat(after.keyKeep()).isEqualTo(Duration.ofMinutes(5));
        assertThat(after.keyUntil()).isNull();
        assertThat(jedis.hpttl(keys.task(1), "ku")).containsExactly(-2L);
        assertThat(jedis.hpttl(keys.task(1), "uk", "kk")).containsExactly(-1L, -1L);
        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("1");
        assertThat(jedis.pttl(keys.uniqueKey("k"))).isEqualTo(-1L);
        assertThat(jedis.zscore(keys.index(5, queue), "1")).isNull();
        assertThat(jedis.zscore(keys.queue(queue, type), "1")).isNotNull();
    }

    @Test
    void retryThrowsAndChangesNothingWhenAnotherTaskHoldsTheKey() {
        var t = enqueued(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)));
        assertThat(store.cancel(1, t.revision(), "x", null)).isEqualTo(CancelOutcome.CANCELLED);
        var cancelled = store.getTask(1).orElseThrow();
        var other = enqueued(task(77));
        jedis.set(keys.uniqueKey("k"), "77");
        var beforeCounts = counts();
        var beforeIndex = jedis.zscore(keys.index(5, queue), "1");

        assertThat(store.retry(1, cancelled.revision() - 1)).isEqualTo(RetryOutcome.STALE);
        assertThatThrownBy(() -> store.retry(1, cancelled.revision())).isInstanceOf(TaskStoreException.class);

        assertThat(store.getTask(1).orElseThrow()).isEqualTo(cancelled);
        assertThat(store.getTask(77).orElseThrow()).isEqualTo(other);
        assertThat(counts()).isEqualTo(beforeCounts);
        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("77");
        assertThat(jedis.zscore(keys.index(5, queue), "1")).isEqualTo(beforeIndex);
        assertThat(jedis.zscore(keys.queue(queue, type), "1")).isNull();
    }

    @Test
    void retryOverwritesAKeyStringWhoseTaskIsGone() {
        var t = enqueued(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)));
        assertThat(store.cancel(1, t.revision(), "x", null)).isEqualTo(CancelOutcome.CANCELLED);
        var cancelled = store.getTask(1).orElseThrow();
        jedis.set(keys.uniqueKey("k"), "88");

        assertThat(store.retry(1, cancelled.revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("1");
    }

    @Test
    void retryRefusesAWrongRevisionAndNonTerminalStates() {
        var t = leased(2);
        assertThat(store.finishCancelled(2, t.revision(), "x", true, null)).isTrue();
        var cancelled = store.getTask(2).orElseThrow();
        var queued = enqueued(1);

        assertThat(store.retry(1, queued.revision())).isEqualTo(RetryOutcome.NOT_RETRYABLE);
        assertThat(store.retry(2, cancelled.revision() - 1)).isEqualTo(RetryOutcome.STALE);
        assertThat(store.retry(99, 0)).isEqualTo(RetryOutcome.NOT_FOUND);

        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.CANCELLED);
        assertThat(counts().cancelled()).isEqualTo(1);
    }

    @Test
    void deleteRemovesATaskFromWhereverItIs() {
        var done = RedisFixtures.leasedWithHistory(store, queue, leased(task(3).encodedPayload("p")));
        store.ack(3, done.revision(), true, true, null);
        var ended = RedisFixtures.leasedWithHistory(
                store,
                queue,
                leased(task(4).uniqueKey("w").keyKeep(Duration.ofMinutes(5)).encodedPayload("p")));
        store.ack(4, ended.revision(), true, true, null);
        var waiting = leased(
                task(1).startBy(Instant.now().plusSeconds(600)).uniqueKey("k").encodedPayload("p"));
        RedisFixtures.addHistory(store, waiting, Instant.now().minusSeconds(5));
        var later = leased(task(2).encodedPayload("p"));
        RedisFixtures.addHistory(store, later, Instant.now().plusSeconds(600));
        assertThat(store.promote(10).moved()).isEqualTo(1);
        for (var id = 1; id <= 4; id++) {
            RedisFixtures.assertPopulated(jedis, keys, id);
        }
        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.getTask(3).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(store.getTask(4).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(jedis.zscore(keys.startBy, "1")).isNotNull();
        assertThat(jedis.exists(keys.uniqueKey("k"), keys.uniqueKey("w"))).isEqualTo(2);

        for (var id = 1; id <= 4; id++) {
            assertThat(store.delete(id, store.getTask(id).orElseThrow().revision(), null))
                    .as("delete %d", id)
                    .isEqualTo(DeleteOutcome.DELETED);
        }

        for (var id = 1; id <= 4; id++) {
            RedisFixtures.assertGone(jedis, keys, id);
        }
        assertThat(jedis.exists(keys.uniqueKey("k"), keys.uniqueKey("w"))).isZero();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
    }

    @Test
    void deleteRefusesALeasedTaskAndAWrongRevision() {
        var t = leased(1);
        var q = enqueued(2);

        assertThat(store.delete(1, t.revision(), null)).isEqualTo(DeleteOutcome.LEASED);
        assertThat(store.delete(2, q.revision() + 1, null)).isEqualTo(DeleteOutcome.STALE);
        assertThat(store.delete(99, 0, null)).isEqualTo(DeleteOutcome.NOT_FOUND);

        assertThat(store.getTask(1)).isPresent();
        assertThat(store.getTask(2)).isPresent();
        assertThat(counts().leased()).isEqualTo(1);
        assertThat(counts().queued()).isEqualTo(1);
    }

    @Test
    void typeQueueEmptiesOnLastRemovalByClaimCancelDeleteExpireAndPurge() {
        var otherType = store.typeId("other");
        enqueued(task(99).typeId(otherType));
        for (var action : List.of("claim", "cancel", "delete", "expire")) {
            var id = 1L;
            var t = enqueued(task(id).startBy(action.contains("expire") ? Instant.EPOCH : null));
            assertThat(queuedTypes()).containsExactlyInAnyOrder(type, otherType);
            switch (action) {
                case "claim" -> {
                    var leased = store.claim(
                                    new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, Set.of(type)))
                            .getFirst();
                    store.ack(id, leased.revision(), true, true, null);
                }
                case "cancel" ->
                    assertThat(store.cancel(id, t.revision(), "stop", null)).isEqualTo(CancelOutcome.CANCELLED);
                case "delete" ->
                    assertThat(store.delete(id, t.revision(), null)).isEqualTo(DeleteOutcome.DELETED);
                case "expire" ->
                    assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(id);
                default -> throw new AssertionError(action);
            }
            assertThat(jedis.exists(keys.queue(queue, type))).as(action).isFalse();
            assertThat(queuedTypes()).as(action).containsExactly(otherType);
            for (var state : List.of(TaskState.SUCCEEDED, TaskState.CANCELLED, TaskState.EXPIRED)) {
                store.purge(state, Duration.ZERO, 10);
            }
            assertThat(store.getTask(id)).isEmpty();
            assertThat(store.queuedByType()).isEqualTo(Map.of(queue, Map.of(otherType, 1L)));
        }
        assertThat(store.delete(99, 0, null)).isEqualTo(DeleteOutcome.DELETED);
        assertThat(store.queuedByType()).doesNotContainKey(queue);
    }

    @Test
    void typeQueueRefillsOnUnclaimPromoteAndRetry() {
        var t = leased(1);
        assertThat(queuedTypes()).isEmpty();
        assertThat(store.unclaim(1, t.revision())).isTrue();
        assertThat(queuedTypes()).containsExactly(type);
        t = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, null))
                .getFirst();
        assertThat(store.reschedule(1, t.revision(), Instant.EPOCH)).isTrue();
        assertThat(queuedTypes()).isEmpty();
        assertThat(store.promote(10).moved()).isEqualTo(1);
        assertThat(queuedTypes()).containsExactly(type);
        var waiting = store.getTask(1).orElseThrow();
        store.cancel(1, waiting.revision(), "stop", null);
        assertThat(queuedTypes()).isEmpty();
        assertThat(store.retry(1, waiting.revision() + 1)).isEqualTo(RetryOutcome.RETRIED);
        assertThat(queuedTypes()).containsExactly(type);
        assertThat(store.queuedByType()).isEqualTo(Map.of(queue, Map.of(type, 1L)));
    }

    // ---- runners -------------------------------------------------------------------------------------

    @Test
    void runnersAreListedInIdOrderWithHeartbeats() {
        var a = store.allocateRunnerId("a", Set.of(), Set.of());
        var b = store.allocateRunnerId("b", Set.of(), Set.of());
        assertThat(store.runners()).extracting(RunnerRecord::running).containsOnly(0);
        assertThat(store.runners()).extracting(RunnerRecord::accepting).containsOnly(true);
        var registered = store.runners().getFirst();
        assertThat(registered.startedAt()).isNotNull();
        assertThat(registered.lastSeen()).isNotNull();

        store.heartbeat(b, "b", Set.of(), Set.of(), 4, false);

        var runners = store.runners();
        assertThat(runners).extracting(RunnerRecord::id).containsExactly(a, b);
        assertThat(runners).extracting(RunnerRecord::label).containsExactly("a", "b");
        assertThat(runners.get(1).running()).isEqualTo(4);
        assertThat(runners.get(1).accepting()).isFalse();
        store.heartbeat(b, "b", Set.of(), Set.of(), 2, true);
        assertThat(store.runners().get(1).accepting()).isTrue();
        assertThat(runners.get(1).lastSeen()).isAfterOrEqualTo(runners.get(1).startedAt());
        assertThat(jedis.zscore(keys.runners, String.valueOf(b))).isNotNull();
    }

    @Test
    void purgeRunnersRemovesOnlyStaleOnes() {
        var stale = store.allocateRunnerId("stale", Set.of(), Set.of());
        var fresh = store.allocateRunnerId("fresh", Set.of(), Set.of());
        jedis.zadd(keys.runners, Instant.now().minusSeconds(3600).toEpochMilli(), String.valueOf(stale));

        assertThat(store.purgeRunners(Duration.ofMinutes(10))).isEqualTo(1);

        assertThat(store.runners()).extracting(RunnerRecord::id).containsExactly(fresh);
        assertThat(jedis.exists(keys.runner(stale))).isFalse();
        assertThat(store.purgeRunners(Duration.ofMinutes(10))).isZero();
        store.heartbeat(stale, "stale", Set.of(), Set.of(), 1, true);
        assertThat(store.runners()).extracting(RunnerRecord::id).containsExactly(stale, fresh);
    }

    // ---- recurring -----------------------------------------------------------------------------------

    private StoredSchedule cron(String id) {
        return new StoredSchedule(id, type, 2, queue, "acme", "* * * * *", "{\"d\":1}", 5, 0);
    }

    @Test
    void recurringDefinitionsRoundTripEveryField() {
        var fire = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        var full = new StoredSchedule(
                "full",
                type,
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
                null,
                false,
                0);
        var delay = new StoredSchedule(
                "delay",
                type,
                1,
                queue,
                null,
                null,
                null,
                null,
                0,
                Duration.ofSeconds(30),
                false,
                null,
                1,
                null,
                true,
                0);

        store.putSchedule(delay, fire.plusSeconds(1), fire.plusSeconds(1));
        store.putSchedule(full, fire, fire);

        var listed = store.schedules();
        assertThat(listed).extracting(r -> r.definition().scheduleId()).containsExactly("delay", "full");
        assertThat(listed.get(1).nextFireAt()).isEqualTo(fire);
        assertThat(listed.get(1).definition().withRevision(0)).isEqualTo(full);
        assertThat(listed.get(1).definition().revision()).isPositive();
        assertThat(listed.get(0).definition().withRevision(0)).isEqualTo(delay);
        assertThat(listed.get(0).nextFireAt()).isEqualTo(fire.plusSeconds(1));
        assertThat(jedis.hkeys(keys.cronDefinition("delay")))
                .doesNotContain("cron", "zone", "mf", "cum", "os", "acc", "data");
        assertThat(store.nextFireTimes()).containsOnlyKeys("delay", "full");
    }

    @Test
    void pausedDefinitionsAreNotDueAndKeepTheirFireTime() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        store.putSchedule(cron("s"), fire, fire);

        assertThat(store.pauseSchedule("s")).isTrue();

        assertThat(store.dueSchedules(Instant.now(), 10)).isEmpty();
        assertThat(jedis.zscore(keys.cron, "s")).isNull();
        assertThat(store.schedules().getFirst().definition().paused()).isTrue();
        assertThat(store.schedules().getFirst().nextFireAt()).isEqualTo(fire);

        assertThat(store.resumeSchedule("s", null)).isTrue();

        var due = store.dueSchedules(Instant.now(), 10);
        assertThat(due).hasSize(1);
        assertThat(due.getFirst().fireAt()).isEqualTo(fire);
        assertThat(due.getFirst().definition().paused()).isFalse();
        assertThat(store.pauseSchedule("missing")).isFalse();
    }

    @Test
    void putKeepsTheFireTimeOnlyWhenCronZoneAndFixedDelayAreAllUnchanged() {
        var fire = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        var later = fire.plusSeconds(600);
        store.putSchedule(cron("s"), fire, fire);

        store.putSchedule(cron("s"), later, later);
        assertThat(store.nextFireTimes()).containsEntry("s", fire);

        store.putSchedule(zoned("s", "Asia/Kolkata"), later, later);
        assertThat(store.nextFireTimes()).containsEntry("s", later);
        store.putSchedule(zoned("s", "Asia/Kolkata"), fire, later);
        assertThat(store.nextFireTimes()).containsEntry("s", later);

        store.putSchedule(zoned("s", "Europe/Berlin"), fire, fire);
        assertThat(store.nextFireTimes()).containsEntry("s", fire);

        var delay = fixedDelay("d", 1000L);
        store.putSchedule(delay, fire, fire);
        store.putSchedule(delay, later, later);
        assertThat(store.nextFireTimes()).containsEntry("d", fire);
        store.putSchedule(fixedDelay("d", 2000L), later, later);
        assertThat(store.nextFireTimes()).containsEntry("d", later);

        store.putSchedule(cron("s"), fire.plusSeconds(1), fire.plusSeconds(1));
        assertThat(store.nextFireTimes()).containsEntry("s", fire.plusSeconds(1));
    }

    @Test
    void definitionRevisionsStayExactBeyondTheLuaDoubleRange() {
        jedis.set(keys.cronRevision, "9007199254740992");
        store.putSchedule(
                cron("s"), Instant.now().plusSeconds(60), Instant.now().plusSeconds(60));
        assertThat(jedis.hget(keys.cronDefinition("s"), "rev")).isEqualTo("9007199254740993");
        assertThat(store.schedules().get(0).definition().revision()).isEqualTo(9007199254740993L);
    }

    private StoredSchedule zoned(String id, String zone) {
        return new StoredSchedule(
                id, type, 2, queue, "acme", "* * * * *", zone, null, 0, null, false, null, 5, null, false, 0);
    }

    private StoredSchedule fixedDelay(String id, long ms) {
        return new StoredSchedule(
                id, type, 2, queue, null, null, null, null, 0, Duration.ofMillis(ms), false, null, 5, null, false, 0);
    }

    @Test
    void replacingADefinitionKeepsItsPausedFlagAndAZsetPlacement() {
        var fire = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        store.putSchedule(cron("s").withPaused(true), fire, fire);
        assertThat(store.schedules().getFirst().definition().paused()).isTrue();
        assertThat(jedis.zscore(keys.cron, "s")).isNull();

        store.putSchedule(cron("s").withPaused(false), fire, fire);

        assertThat(store.schedules().getFirst().definition().paused()).isTrue();
        assertThat(jedis.zscore(keys.cron, "s")).isNull();

        store.resumeSchedule("s", null);
        store.putSchedule(cron("s").withPaused(true), fire, fire);

        assertThat(store.schedules().getFirst().definition().paused()).isFalse();
        assertThat(jedis.zscore(keys.cron, "s")).isNotNull();
        assertThat(store.dueSchedules(fire, 10)).hasSize(1);
    }

    @Test
    void commitIsRejectedWhenTheDefinitionWasPausedAfterItWasRead() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        store.putSchedule(cron("s"), fire, fire);
        var due = store.dueSchedules(Instant.now(), 10).getFirst();
        store.pauseSchedule("s");

        assertThat(store.commitSchedule(due, fire.plusSeconds(60), task(80).build()))
                .isFalse();

        assertThat(store.getTask(80)).isEmpty();
        assertThat(store.schedules().getFirst().nextFireAt()).isEqualTo(fire);
        store.resumeSchedule("s", null);
        assertThat(store.commitSchedule(due, fire.plusSeconds(60), task(80).build()))
                .isTrue();
    }

    @Test
    void retryOfAnExpiredTaskClearsStartByAndMakesItClaimable() {
        enqueued(task(1).startBy(past()));
        store.expire(10);
        var expired = store.getTask(1).orElseThrow();

        assertThat(store.retry(1, expired.revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(store.getTask(1).orElseThrow().startBy()).isNull();
        assertThat(jedis.zscore(keys.startBy, "1")).isNull();
        assertThat(store.expire(10)).isEmpty();
        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, null)))
                .extracting(TaskRecord::id)
                .containsExactly(1L);
    }

    @Test
    void triggerRecurringReArmsAndRefusesUnknownDefinitions() {
        var fire = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
        store.putSchedule(cron("s"), fire, fire);
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        assertThat(store.triggerSchedule("s", now)).isTrue();

        assertThat(store.nextFireTimes()).containsEntry("s", now);
        assertThat(store.dueSchedules(now.plusSeconds(1), 10)).hasSize(1);
        assertThat(store.triggerSchedule("missing", now)).isFalse();

        store.pauseSchedule("s");
        assertThat(store.triggerSchedule("s", fire)).isTrue();
        assertThat(store.dueSchedules(fire.plusSeconds(1), 10)).isEmpty();
        store.resumeSchedule("s", null);
        assertThat(store.nextFireTimes()).containsEntry("s", fire);
    }

    @Test
    void deleteRecurringRemovesItFromTheListing() {
        store.putSchedule(
                cron("s"), Instant.now().plusSeconds(60), Instant.now().plusSeconds(60));

        assertThat(store.deleteSchedule("s")).isTrue();

        assertThat(store.schedules()).isEmpty();
        assertThat(jedis.scard(keys.cronDefinitions)).isZero();
    }

    @Test
    void commitWithAHeldUniqueKeyAdvancesTheDefinitionWithoutStoringTheOccurrence() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        var overlap = new StoredSchedule(
                "s", type, 2, queue, null, "* * * * *", null, null, 0, null, true, null, 5, null, false, 0);
        store.putSchedule(overlap, fire, fire);
        var first = store.dueSchedules(Instant.now(), 10).getFirst();
        var next1 = fire.plusSeconds(60);
        var next2 = fire.plusSeconds(120);

        assertThat(store.commitSchedule(
                        first, next1, task(70).uniqueKey("sched:s").build()))
                .isTrue();
        var second = new DueSchedule("s", next1, first.definition(), false);
        assertThat(store.commitSchedule(
                        second, next2, task(71).uniqueKey("sched:s").build()))
                .isTrue();

        assertThat(store.getTask(70)).isPresent();
        assertThat(store.getTask(71)).isEmpty();
        assertThat(jedis.zscore(keys.scheduled(queue), "71")).isNull();
        assertThat(jedis.get(keys.uniqueKey("sched:s"))).isEqualTo("70");
        assertThat(counts().scheduled()).isEqualTo(1);
        assertThat(store.nextFireTimes()).containsEntry("s", next2);
    }
}
