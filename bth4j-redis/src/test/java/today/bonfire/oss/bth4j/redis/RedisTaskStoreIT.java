package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
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
import today.bonfire.oss.bth4j.spi.RecordJson;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

/** {@link RedisTaskStore} against a real Redis, including the checks that read Redis keys directly. Needs Docker. */
@Testcontainers
class RedisTaskStoreIT {

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
                .clientName("bth4j-store-test")
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
        store = new RedisTaskStore(jedis, "T" + NAMESPACES.incrementAndGet());
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

    private TaskRecord enqueued(long id) {
        assertThat(store.enqueue(task(id).build())).isEqualTo(EnqueueOutcome.STORED);
        return store.getTask(id).orElseThrow();
    }

    private TaskRecord claimOne(long id) {
        enqueued(id);
        return claim(1).getFirst();
    }

    private List<TaskRecord> claim(int count) {
        return store.claim(new ClaimRequest(List.of(queue), count, LEASE, 1, null));
    }

    private static FailureInfo failure(int attempt) {
        return new FailureInfo(
                attempt, Instant.now().truncatedTo(ChronoUnit.MILLIS), "java.lang.Boom", "boom " + attempt, null);
    }

    private StateCounts counts() {
        return store.counts().getOrDefault(queue, StateCounts.ZERO);
    }

    // ---- identity ------------------------------------------------------------------------------------

    @Test
    void initWritesSchemaAndRefusesAnotherVersion() {
        assertThat(store.info().engine()).isEqualTo("redis");
        assertThat(jedis.hget(keys.meta, "schema")).isEqualTo("1");
        store.init();

        jedis.hset(keys.meta, "schema", "99");

        assertThatThrownBy(store::init).isInstanceOf(TaskStoreException.class);
    }

    @Test
    void idsAreReservedInBlocks() {
        assertThat(store.allocateIds(10)).isEqualTo(1);
        assertThat(store.allocateIds(5)).isEqualTo(11);
        assertThat(store.allocateIds(1)).isEqualTo(16);
        assertThatThrownBy(() -> store.allocateIds(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void workersGetDistinctIdsAndAreRecorded() {
        var a = store.allocateWorkerId("host-a:1");
        var b = store.allocateWorkerId("host-b:2");

        assertThat(a).isNotEqualTo(b);
        assertThat(jedis.hget(keys.worker(a), "label")).isEqualTo("host-a:1");
        assertThat(jedis.hget(keys.worker(a), "started")).isNotNull();
    }

    @Test
    void registriesAreStableAndListed() {
        var other = store.queueId("other");

        assertThat(store.queueId("default")).isEqualTo(queue);
        assertThat(other).isNotEqualTo(queue);
        assertThat(new RedisTaskStore(jedis, keys.namespace).queueId("other")).isEqualTo(other);
        assertThat(store.queueNames()).containsEntry(queue, "default").containsEntry(other, "other");
        assertThat(store.eventNames()).containsEntry(event, "send");
        assertThat(jedis.hvals(keys.registry("queue"))).contains(String.valueOf(queue), String.valueOf(other));
    }

    @Test
    void registryRefusesMoreThan32767Names() {
        jedis.set(keys.prefix + "seq:event", "32767");

        assertThatThrownBy(() -> store.eventId("one too many")).isInstanceOf(TaskStoreException.class);
    }

    // ---- enqueue -------------------------------------------------------------------------------------

    @Test
    void enqueueStoresOnlyNonNullFieldsWithoutExpiry() {
        enqueued(1);

        var hash = jedis.hgetAll(keys.task(1));

        assertThat(hash.keySet())
                .containsExactlyInAnyOrder("rev", "q", "ev", "pv", "state", "att", "max", "created", "due", "sc");
        assertThat(hash).containsEntry("state", "0");
        assertThat(jedis.ttl(keys.task(1))).isEqualTo(-1);
        assertThat(jedis.zscore(keys.queue(queue), "1")).isEqualTo(Double.valueOf(hash.get("due")));
        assertThat(counts().queued()).isEqualTo(1);
    }

    @Test
    void enqueueRoundTripsEveryField() {
        var t = task(7).data("{\"a\":1}")
                .account("acme")
                .headers(Map.of("h", "v"))
                .retryPolicy("slow")
                .scheduleId("sched")
                .retryOf(3L)
                .startBy(Instant.ofEpochMilli(1_900_000_000_000L))
                .maxRun(Duration.ofMinutes(2))
                .payloadVersion(4)
                .build();

        store.enqueue(t);
        var back = store.getTask(7).orElseThrow();

        assertThat(back.data()).isEqualTo("{\"a\":1}");
        assertThat(back.account()).isEqualTo("acme");
        assertThat(back.headers()).containsEntry("h", "v");
        assertThat(back.retryPolicy()).isEqualTo("slow");
        assertThat(back.scheduleId()).isEqualTo("sched");
        assertThat(back.retryOf()).isEqualTo(3L);
        assertThat(back.startBy()).isEqualTo(t.startBy());
        assertThat(back.maxRun()).isEqualTo(Duration.ofMinutes(2));
        assertThat(back.payloadVersion()).isEqualTo(4);
        assertThat(back.created()).isEqualTo(t.created());
        assertThat(back.state()).isEqualTo(TaskState.QUEUED);
        assertThat(back.due()).isEqualTo(back.stateChanged());
    }

    @Test
    void futureDueIsScheduledAndPastDueIsQueued() {
        var future = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        store.enqueue(task(1).due(future).build());
        store.enqueue(task(2).due(Instant.now().minusSeconds(60)).build());

        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.getTask(1).orElseThrow().due()).isEqualTo(future);
        assertThat(store.getTask(2).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(jedis.zscore(keys.scheduled, "1")).isEqualTo((double) future.toEpochMilli());
        assertThat(store.nextDue()).isEqualTo(future);
        assertThat(counts().scheduled()).isEqualTo(1);
    }

    @Test
    void existingIdIsReportedInAnyState() {
        var t = claimOne(1);
        assertThat(store.enqueue(task(1).build())).isEqualTo(EnqueueOutcome.EXISTS);

        store.ack(1, t.revision(), true, true);

        assertThat(store.enqueue(task(1).build())).isEqualTo(EnqueueOutcome.EXISTS);
        assertThat(counts().queued()).isZero();
    }

    @Test
    void duplicateUniqueKeyNamesTheHolder() {
        store.enqueue(task(1).uniqueKey("k").build());

        var outcome = store.enqueue(task(2).uniqueKey("k").build());

        assertThat(outcome).isEqualTo(new EnqueueOutcome.DuplicateKey(1));
        assertThat(store.getTask(2)).isEmpty();
        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("1");
    }

    @Test
    void uniqueKeyIsReleasedWhenTheTaskEnds() {
        store.enqueue(task(1).uniqueKey("k").build());
        var t = claim(1).getFirst();
        assertThat(store.enqueue(task(2).uniqueKey("k").build())).isInstanceOf(EnqueueOutcome.DuplicateKey.class);

        store.ack(1, t.revision(), true, true);

        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
        assertThat(jedis.hexists(keys.task(1), "uk")).isFalse();
        assertThat(store.enqueue(task(2).uniqueKey("k").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void keyKeepWindowHoldsTheKeyViaPexpireAndKeepsTheHashEvenWithoutKeep() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMillis(600)).build());
        store.enqueue(
                task(2).data("p").uniqueKey("j").keyKeep(Duration.ofMillis(600)).build());
        var claimed = claim(2);

        store.ack(1, claimed.get(0).revision(), true, true);
        store.ack(2, claimed.get(1).revision(), false, false);

        assertThat(jedis.pttl(keys.uniqueKey("k"))).isBetween(1L, 600L);
        assertThat(jedis.pttl(keys.uniqueKey("j"))).isBetween(1L, 600L);
        assertThat(store.getTask(1).orElseThrow().keyUntil()).isNotNull();
        var kept = store.getTask(2).orElseThrow();
        assertThat(kept.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(kept.data()).isNull();
        assertThat(kept.keyUntil()).isNotNull();
        assertThat(store.enqueue(task(3).uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(1));
        assertThat(store.enqueue(task(4).uniqueKey("j").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(2));
        sleep(700);
        assertThat(store.enqueue(task(3).uniqueKey("k").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void releaseKeysClearsTheKeyFieldsOfFinishedTasks() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMillis(200)).build());
        var t = claim(1).getFirst();
        store.ack(1, t.revision(), true, true);
        assertThat(store.releaseKeys(10)).isZero();
        sleep(300);

        assertThat(store.releaseKeys(10)).isEqualTo(1);

        var back = store.getTask(1).orElseThrow();
        assertThat(back.uniqueKey()).isNull();
        assertThat(back.keyKeep()).isNull();
        assertThat(back.keyUntil()).isNull();
        assertThat(store.releaseKeys(10)).isZero();
    }

    @Test
    void enqueueAllReportsEachOutcomeInOrderAndSurvivesScriptFlush() {
        store.enqueue(task(2).build());
        jedis.scriptFlush();

        var outcomes = store.enqueueAll(List.of(
                task(1).uniqueKey("k").build(),
                task(2).build(),
                task(3).uniqueKey("k").build(),
                task(4).build()));

        assertThat(outcomes)
                .containsExactly(
                        EnqueueOutcome.STORED,
                        EnqueueOutcome.EXISTS,
                        new EnqueueOutcome.DuplicateKey(1),
                        EnqueueOutcome.STORED);
        assertThat(store.enqueueAll(List.of())).isEmpty();
    }

    // ---- claim ---------------------------------------------------------------------------------------

    @Test
    void claimLeasesInDueOrderAndBumpsRevisionAndAttempts() {
        var now = Instant.now().minusSeconds(100);
        store.enqueue(task(1).due(now.plusSeconds(10)).build());
        store.enqueue(task(2).due(now).build());
        store.enqueue(task(3).due(now.plusSeconds(20)).build());
        var before = store.getTask(1).orElseThrow();

        var claimed = claim(2);

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(2L, 1L);
        var first = claimed.get(1);
        assertThat(first.state()).isEqualTo(TaskState.LEASED);
        assertThat(first.revision()).isEqualTo(before.revision() + 1);
        assertThat(first.attempts()).isEqualTo(1);
        assertThat(first.workerId()).isEqualTo(1);
        assertThat(first.claimedAt()).isNotNull();
        assertThat(first.leaseUntil()).isAfter(Instant.now().plusSeconds(25));
        assertThat(first.due()).isEqualTo(before.due());
        assertThat(jedis.zscore(keys.leased, "1"))
                .isEqualTo((double) first.leaseUntil().toEpochMilli());
        assertThat(jedis.zscore(keys.index(2, queue), "2")).isNotNull();
        assertThat(jedis.zscore(keys.queue(queue), "1")).isNull();
        assertThat(counts().queued()).isEqualTo(1);
        assertThat(counts().leased()).isEqualTo(2);
    }

    @Test
    void claimReadsQueuesInTheGivenOrderOnlyAsNeeded() {
        var other = store.queueId("other");
        store.enqueue(task(1).build());
        store.enqueue(task(2).queueId(other).build());
        store.enqueue(task(3).queueId(other).build());

        var one = store.claim(new ClaimRequest(List.of(other, queue), 1, LEASE, 1, null));
        var rest = store.claim(new ClaimRequest(List.of(other, queue), 5, LEASE, 1, null));

        assertThat(one).extracting(TaskRecord::id).containsExactly(2L);
        assertThat(rest).extracting(TaskRecord::id).containsExactly(3L, 1L);
        assertThat(store.claim(new ClaimRequest(List.of(queue), 0, LEASE, 1, null)))
                .isEmpty();
    }

    @Test
    void claimDoesNotTakeScheduledTasks() {
        store.enqueue(task(1).due(Instant.now().plusSeconds(60)).build());

        assertThat(claim(5)).isEmpty();
    }

    @Test
    void unknownEventsAreDeferredFiveSecondsWithoutAnAttempt() {
        var unknown = store.eventId("unknown");
        store.enqueue(task(1).eventId(unknown).build());
        store.enqueue(task(2).build());
        var before = System.currentTimeMillis();

        var claimed = store.claim(new ClaimRequest(List.of(queue), 5, LEASE, 1, Set.of(event)));

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(2L);
        var deferred = store.getTask(1).orElseThrow();
        assertThat(deferred.attempts()).isZero();
        assertThat(deferred.revision()).isEqualTo(0);
        assertThat(deferred.state()).isEqualTo(TaskState.QUEUED);
        assertThat(jedis.zscore(keys.queue(queue), "1")).isBetween(before + 4_500d, before + 6_500d);
        assertThat(store.claim(new ClaimRequest(List.of(queue), 5, LEASE, 1, null)))
                .isEmpty();
        assertThat(store.claim(new ClaimRequest(List.of(queue), 5, LEASE, 1, Set.of())))
                .isEmpty();
    }

    @Test
    void concurrentClaimsNeverHandOutATaskTwice() throws Exception {
        var total = 300;
        var ids = store.allocateIds(total);
        var tasks = new ArrayList<TaskRecord>();
        for (var i = 0; i < total; i++) {
            tasks.add(task(ids + i).build());
        }
        store.enqueueAll(tasks);
        var pool = Executors.newFixedThreadPool(8);
        try {
            var calls = new ArrayList<Callable<List<TaskRecord>>>();
            for (var w = 0; w < 8; w++) {
                var worker = w;
                calls.add(() -> {
                    var mine = new ArrayList<TaskRecord>();
                    List<TaskRecord> got;
                    do {
                        got = store.claim(new ClaimRequest(List.of(queue), 7, LEASE, worker, null));
                        mine.addAll(got);
                    } while (!got.isEmpty());
                    return mine;
                });
            }
            var seen = new HashSet<Long>();
            var count = 0;
            for (var f : pool.invokeAll(calls)) {
                for (var t : f.get()) {
                    seen.add(t.id());
                    count++;
                }
            }
            assertThat(count).isEqualTo(total);
            assertThat(seen).hasSize(total);
            assertThat(counts().leased()).isEqualTo(total);
            assertThat(counts().queued()).isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    // ---- lease, renew, steps -------------------------------------------------------------------------

    @Test
    void renewExtendsOwnedLeasesAndFlagsCancelRequests() {
        enqueued(1);
        enqueued(2);
        enqueued(3);
        var claimed = claim(3);
        jedis.hset(keys.task(2), "cra", "1");
        var shortLease = Duration.ofMinutes(10);

        var statuses = store.renew(
                List.of(
                        new LeaseRef(1, claimed.get(0).revision()),
                        new LeaseRef(2, claimed.get(1).revision()),
                        new LeaseRef(3, claimed.get(2).revision() + 1),
                        new LeaseRef(99, 1)),
                shortLease);

        assertThat(statuses)
                .extracting("id", "owned", "cancelRequested")
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(1L, true, false),
                        org.assertj.core.groups.Tuple.tuple(2L, true, true),
                        org.assertj.core.groups.Tuple.tuple(3L, false, false),
                        org.assertj.core.groups.Tuple.tuple(99L, false, false));
        var renewed = store.getTask(1).orElseThrow();
        assertThat(renewed.revision()).isEqualTo(claimed.get(0).revision());
        assertThat(renewed.leaseUntil()).isAfter(claimed.get(0).leaseUntil());
        assertThat(jedis.zscore(keys.leased, "1"))
                .isEqualTo((double) renewed.leaseUntil().toEpochMilli());
        assertThat(store.renew(List.of(), shortLease)).isEmpty();
    }

    @Test
    void stepsAndProgressDoNotBumpTheRevisionAndAreFenced() {
        var t = claimOne(1);
        var steps = Map.of("a", new StepResult(Instant.ofEpochMilli(1_800_000_000_000L), "{\"x\":1}"));

        assertThat(store.saveSteps(1, t.revision(), steps)).isTrue();
        assertThat(store.progress(1, t.revision(), "half")).isTrue();
        assertThat(store.saveSteps(1, t.revision() + 1, Map.of())).isFalse();
        assertThat(store.progress(1, t.revision() + 1, "x")).isFalse();

        var back = store.getTask(1).orElseThrow();
        assertThat(back.revision()).isEqualTo(t.revision());
        assertThat(back.steps()).isEqualTo(steps);
        assertThat(back.progress()).isEqualTo("half");
        assertThat(store.progress(1, t.revision(), null)).isTrue();
        assertThat(store.getTask(1).orElseThrow().progress()).isNull();
        assertThat(store.saveSteps(1, t.revision(), null)).isTrue();
        assertThat(store.getTask(1).orElseThrow().steps()).isNull();
    }

    @Test
    void stepsAreRejectedOnTasksThatAreNotLeased() {
        enqueued(1);

        assertThat(store.saveSteps(1, 0, Map.of("a", new StepResult(Instant.now(), null))))
                .isFalse();
        assertThat(store.progress(1, 0, "p")).isFalse();
    }

    // ---- ack -----------------------------------------------------------------------------------------

    @Test
    void ackWithoutKeepRemovesTheTaskCompletely() {
        var t = claimOne(1);

        assertThat(store.ack(1, t.revision(), false, false)).isTrue();

        assertThat(store.getTask(1)).isEmpty();
        assertThat(jedis.zscore(keys.leased, "1")).isNull();
        assertThat(jedis.zscore(keys.index(2, queue), "1")).isNull();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
    }

    @Test
    void ackWithKeepSucceedsAndDropsDataUnlessKept() {
        store.enqueue(task(1).data("payload").build());
        store.enqueue(task(2).data("payload").build());
        var claimed = claim(2);
        store.saveSteps(1, claimed.get(0).revision(), Map.of("s", new StepResult(Instant.ofEpochMilli(5), "r")));

        assertThat(store.ack(1, claimed.get(0).revision(), true, false)).isTrue();
        assertThat(store.ack(2, claimed.get(1).revision(), true, true)).isTrue();

        var dropped = store.getTask(1).orElseThrow();
        assertThat(dropped.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(dropped.data()).isNull();
        assertThat(dropped.steps()).containsKey("s");
        assertThat(dropped.leaseUntil()).isNull();
        assertThat(dropped.workerId()).isEqualTo(1);
        assertThat(dropped.revision()).isEqualTo(claimed.get(0).revision() + 1);
        assertThat(store.getTask(2).orElseThrow().data()).isEqualTo("payload");
        assertThat(jedis.zscore(keys.index(3, queue), "1"))
                .isEqualTo((double) dropped.stateChanged().toEpochMilli());
        assertThat(counts().succeeded()).isEqualTo(2);
        assertThat(jedis.ttl(keys.task(1))).isEqualTo(-1);
    }

    // ---- fail ----------------------------------------------------------------------------------------

    @Test
    void failSchedulesRetryWhileAttemptsRemain() {
        var t = claimOne(1);
        var retryAt = Instant.now().plusSeconds(30).truncatedTo(ChronoUnit.MILLIS);

        assertThat(store.fail(1, t.revision(), failure(1), retryAt, true)).isEqualTo(FailOutcome.RETRY);

        var back = store.getTask(1).orElseThrow();
        assertThat(back.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(back.due()).isEqualTo(retryAt);
        assertThat(back.revision()).isEqualTo(t.revision() + 1);
        assertThat(back.leaseUntil()).isNull();
        assertThat(back.workerId()).isNull();
        assertThat(back.failures()).hasSize(1);
        assertThat(back.lastFailure().message()).isEqualTo("boom 1");
        assertThat(jedis.zscore(keys.scheduled, "1")).isEqualTo((double) retryAt.toEpochMilli());
        assertThat(jedis.zscore(keys.leased, "1")).isNull();
        assertThat(counts().scheduled()).isEqualTo(1);
        assertThat(counts().leased()).isZero();
    }

    @Test
    void failIsDeadWithoutRetryAtOrWhenAttemptsAreExhausted() {
        var t = claimOne(1);
        assertThat(store.fail(1, t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.DEAD);
        assertThat(jedis.zscore(keys.index(4, queue), "1")).isNotNull();
        assertThat(counts().dead()).isEqualTo(1);

        store.enqueue(task(2).maxAttempts(1).build());
        var u = claim(1).getFirst();

        assertThat(store.fail(2, u.revision(), failure(1), Instant.now(), true)).isEqualTo(FailOutcome.DEAD);
    }

    @Test
    void deadTaskIsRemovedWhenNotKept() {
        var t = claimOne(1);

        assertThat(store.fail(1, t.revision(), failure(1), null, false)).isEqualTo(FailOutcome.DEAD);

        assertThat(store.getTask(1)).isEmpty();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
        assertThat(jedis.zscore(keys.index(2, queue), "1")).isNull();
    }

    @Test
    void deadTaskWithKeyWindowIsKeptEvenWhenNotKept() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(1)).build());
        var t = claim(1).getFirst();

        assertThat(store.fail(1, t.revision(), failure(1), null, false)).isEqualTo(FailOutcome.DEAD);

        var back = store.getTask(1).orElseThrow();
        assertThat(back.state()).isEqualTo(TaskState.DEAD);
        assertThat(back.keyUntil()).isNotNull();
        assertThat(back.workerId()).isEqualTo(1);
        assertThat(store.purge(TaskState.DEAD, Duration.ZERO, 10)).isZero();
    }

    @Test
    void deadTaskReleasesItsKeyLikeAnAck() {
        store.enqueue(task(1).uniqueKey("k").build());
        store.enqueue(task(2).uniqueKey("j").keyKeep(Duration.ofMinutes(1)).build());
        var claimed = claim(2);

        store.fail(1, claimed.get(0).revision(), failure(1), null, true);
        store.fail(2, claimed.get(1).revision(), failure(1), null, true);

        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
        assertThat(jedis.pttl(keys.uniqueKey("j"))).isPositive();
    }

    @Test
    void failuresAreBoundedToTwentyAndStayParseable() {
        store.enqueue(task(1).maxAttempts(100).build());
        var nasty = "q\"uo\\te ] [ } { , \n tab\t";
        for (var attempt = 1; attempt <= 25; attempt++) {
            var t = claim(1).getFirst();
            var f = new FailureInfo(attempt, Instant.ofEpochMilli(1_000L + attempt), "T", nasty + attempt, nasty);
            assertThat(store.fail(1, t.revision(), f, Instant.now().minusSeconds(1), true))
                    .isEqualTo(FailOutcome.RETRY);
            assertThat(store.promote(10).moved()).isEqualTo(1);
        }

        var failures = RecordJson.decodeFailures(jedis.hget(keys.task(1), "fl"));

        assertThat(failures).hasSize(20);
        assertThat(failures.getFirst().attempt()).isEqualTo(6);
        assertThat(failures.getLast().attempt()).isEqualTo(25);
        assertThat(failures.getLast().stack()).isEqualTo(nasty);
        assertThat(store.getTask(1).orElseThrow().failures()).hasSize(20);
    }

    @Test
    void failExpiredIsLostWhileTheLeaseHoldsAndWorksOnceItExpired() {
        enqueued(1);
        var t = store.claim(new ClaimRequest(List.of(queue), 1, Duration.ofMillis(300), 1, null))
                .getFirst();
        assertThat(store.failExpired(1, t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.LOST);
        assertThat(store.reclaim(10)).isEmpty();
        sleep(400);

        var expired = store.reclaim(10);

        assertThat(expired).hasSize(1);
        var e = expired.getFirst();
        assertThat(e.id()).isEqualTo(1);
        assertThat(e.revision()).isEqualTo(t.revision());
        assertThat(e.attempts()).isEqualTo(1);
        assertThat(e.maxAttempts()).isEqualTo(3);
        assertThat(e.eventId()).isEqualTo(event);
        assertThat(e.cancelRequested()).isFalse();
        assertThat(store.getTask(1).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(store.failExpired(1, e.revision(), failure(1), Instant.now().plusSeconds(5), true))
                .isEqualTo(FailOutcome.RETRY);
    }

    @Test
    void renewedLeaseIsNotReclaimedAsExpired() {
        enqueued(1);
        var t = store.claim(new ClaimRequest(List.of(queue), 1, Duration.ofMillis(300), 1, null))
                .getFirst();
        sleep(400);
        assertThat(store.reclaim(10)).hasSize(1);

        store.renew(List.of(new LeaseRef(1, t.revision())), LEASE);

        assertThat(store.failExpired(1, t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.LOST);
        assertThat(store.reclaim(10)).isEmpty();
    }

    // ---- give back -----------------------------------------------------------------------------------

    @Test
    void rescheduleGivesTheAttemptBackAndMovesToScheduled() {
        var t = claimOne(1);
        var due = Instant.now().plusSeconds(90).truncatedTo(ChronoUnit.MILLIS);

        assertThat(store.reschedule(1, t.revision(), due)).isTrue();

        var back = store.getTask(1).orElseThrow();
        assertThat(back.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(back.attempts()).isZero();
        assertThat(back.due()).isEqualTo(due);
        assertThat(back.workerId()).isNull();
        assertThat(back.leaseUntil()).isNull();
        assertThat(back.revision()).isEqualTo(t.revision() + 1);
        assertThat(jedis.zscore(keys.scheduled, "1")).isEqualTo((double) due.toEpochMilli());
        assertThat(counts().scheduled()).isEqualTo(1);
        assertThat(counts().leased()).isZero();
    }

    @Test
    void unclaimReturnsTheTaskToItsQueueAtItsOldDue() {
        var original = enqueued(1);
        var t = claim(1).getFirst();

        assertThat(store.unclaim(1, t.revision())).isTrue();

        var back = store.getTask(1).orElseThrow();
        assertThat(back.state()).isEqualTo(TaskState.QUEUED);
        assertThat(back.attempts()).isZero();
        assertThat(back.due()).isEqualTo(original.due());
        assertThat(back.workerId()).isNull();
        assertThat(back.leaseUntil()).isNull();
        assertThat(back.revision()).isEqualTo(t.revision() + 1);
        assertThat(jedis.zscore(keys.queue(queue), "1"))
                .isEqualTo((double) original.due().toEpochMilli());
        assertThat(claim(1)).hasSize(1);
    }

    @Test
    void everyTransitionIsFencedByRevisionAndState() {
        var t = claimOne(1);
        var stale = t.revision() - 1;
        var wrong = t.revision() + 1;
        for (var rev : new int[] {stale, wrong}) {
            assertThat(store.ack(1, rev, true, true)).isFalse();
            assertThat(store.fail(1, rev, failure(1), null, true)).isEqualTo(FailOutcome.LOST);
            assertThat(store.failExpired(1, rev, failure(1), null, true)).isEqualTo(FailOutcome.LOST);
            assertThat(store.reschedule(1, rev, Instant.now())).isFalse();
            assertThat(store.unclaim(1, rev)).isFalse();
            assertThat(store.saveSteps(1, rev, Map.of())).isFalse();
            assertThat(store.progress(1, rev, "p")).isFalse();
            assertThat(store.renew(List.of(new LeaseRef(1, rev)), LEASE)
                            .getFirst()
                            .owned())
                    .isFalse();
        }
        assertThat(store.getTask(1).orElseThrow().revision()).isEqualTo(t.revision());

        assertThat(store.unclaim(1, t.revision())).isTrue();
        assertThat(store.ack(1, t.revision(), true, true)).isFalse();
        assertThat(store.fail(1, t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.LOST);
        assertThat(store.ack(404, 0, true, true)).isFalse();
    }

    // ---- maintenance ---------------------------------------------------------------------------------

    @Test
    void promoteMovesDueTasksKeepingTheirDueAndReportsTheNext() {
        var soon = Instant.now().plusMillis(300).truncatedTo(ChronoUnit.MILLIS);
        var later = Instant.now().plusSeconds(300).truncatedTo(ChronoUnit.MILLIS);
        store.enqueue(task(1).due(soon).build());
        store.enqueue(task(2).due(later).build());
        assertThat(store.promote(10).moved()).isZero();
        sleep(400);

        var result = store.promote(10);

        assertThat(result.moved()).isEqualTo(1);
        assertThat(result.nextDue()).isEqualTo(later);
        var moved = store.getTask(1).orElseThrow();
        assertThat(moved.state()).isEqualTo(TaskState.QUEUED);
        assertThat(moved.due()).isEqualTo(soon);
        assertThat(moved.revision()).isEqualTo(1);
        assertThat(jedis.zscore(keys.queue(queue), "1")).isEqualTo((double) soon.toEpochMilli());
        assertThat(store.promote(10).nextDue()).isEqualTo(later);
        assertThat(counts().queued()).isEqualTo(1);
        assertThat(counts().scheduled()).isEqualTo(1);
    }

    @Test
    void promoteHonoursTheLimitAndNextDueIsNullWhenNothingIsScheduled() {
        assertThat(store.nextDue()).isNull();
        for (var i = 1; i <= 3; i++) {
            store.enqueue(task(i).due(Instant.now().plusMillis(100)).build());
        }
        sleep(200);

        assertThat(store.promote(2).moved()).isEqualTo(2);
        assertThat(store.promote(2).moved()).isEqualTo(1);
        assertThat(store.promote(2)).isEqualTo(new today.bonfire.oss.bth4j.spi.PromoteResult(0, null));
    }

    @Test
    void purgeRemovesOldTerminalTasksOnlyAndKeepsHeldKeys() {
        store.enqueue(task(1).build());
        store.enqueue(task(2).build());
        store.enqueue(task(3).uniqueKey("k").keyKeep(Duration.ofMinutes(5)).build());
        store.enqueue(task(4).build());
        var claimed = claim(4);
        store.ack(1, claimed.get(0).revision(), true, true);
        store.ack(2, claimed.get(1).revision(), true, true);
        store.ack(3, claimed.get(2).revision(), true, true);
        store.fail(4, claimed.get(3).revision(), failure(1), null, true);
        sleep(150);

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMinutes(1), 10)).isZero();
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10)).isEqualTo(2);

        assertThat(store.getTask(1)).isEmpty();
        assertThat(store.getTask(3)).isPresent();
        assertThat(store.getTask(4)).isPresent();
        assertThat(jedis.exists(keys.uniqueKey("k"))).isTrue();
        assertThat(counts().succeeded()).isEqualTo(1);
        assertThat(counts().dead()).isEqualTo(1);
        assertThat(store.purge(TaskState.DEAD, Duration.ofMillis(100), 10)).isEqualTo(1);
        assertThatThrownBy(() -> store.purge(TaskState.QUEUED, Duration.ZERO, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void purgeHonoursTheLimitAndHeldTasksDoNotStarveIt() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)).build());
        for (var i = 2; i <= 5; i++) {
            store.enqueue(task(i).build());
        }
        for (var t : claim(5)) {
            store.ack(t.id(), t.revision(), true, true);
        }
        sleep(50);

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(2);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isEqualTo(2);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isZero();
        assertThat(store.getTask(1)).isPresent();
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Test
    void countsCoverEveryQueueAndState() {
        var other = store.queueId("other");
        store.enqueue(task(1).build());
        store.enqueue(task(2).due(Instant.now().plusSeconds(60)).build());
        store.enqueue(task(3).queueId(other).build());

        var counts = store.counts();

        assertThat(counts.get(queue)).isEqualTo(new StateCounts(1, 1, 0, 0, 0, 0, 0));
        assertThat(counts.get(other).queued()).isEqualTo(1);
        assertThat(counts.get(queue).of(TaskState.SCHEDULED)).isEqualTo(1);
    }

    @Test
    void getTaskIsEmptyForUnknownIds() {
        assertThat(store.getTask(12345)).isEmpty();
    }

    @Test
    void terminalTasksListInKeysetPagesAcrossQueues() {
        var other = store.queueId("other");
        var done = new ArrayList<Long>();
        for (var i = 1; i <= 12; i++) {
            store.enqueue(task(i).queueId(i % 3 == 0 ? other : queue).build());
        }
        for (var t : store.claim(new ClaimRequest(List.of(queue, other), 12, LEASE, 1, null))) {
            store.ack(t.id(), t.revision(), true, true);
        }

        long after = 0;
        List<TaskRecord> page;
        do {
            page = store.tasks(TaskState.SUCCEEDED, null, after, 5);
            assertThat(page.size()).isLessThanOrEqualTo(5);
            page.forEach(t -> done.add(t.id()));
            after = page.isEmpty() ? after : page.getLast().id();
        } while (!page.isEmpty());

        assertThat(done).hasSize(12).doesNotHaveDuplicates();
        var byTime = new ArrayList<TaskRecord>();
        done.forEach(id -> byTime.add(store.getTask(id).orElseThrow()));
        assertThat(byTime)
                .isSortedAccordingTo(
                        java.util.Comparator.comparing(TaskRecord::stateChanged).thenComparingLong(TaskRecord::id));
        assertThat(store.tasks(TaskState.SUCCEEDED, other, 0, 100))
                .extracting(TaskRecord::id)
                .containsExactlyInAnyOrder(3L, 6L, 9L, 12L);
        assertThat(store.tasks(TaskState.DEAD, null, 0, 10)).isEmpty();
        assertThat(store.tasks(TaskState.SUCCEEDED, null, 0, 0)).isEmpty();
    }

    @Test
    void pagingCrossesChunkBoundariesWithoutLosingOrDuplicatingTasks() {
        var first = store.allocateIds(1100);
        var tasks = new ArrayList<TaskRecord>();
        for (var i = 0; i < 1100; i++) {
            tasks.add(task(first + i).build());
        }
        store.enqueueAll(tasks);

        var seen = new ArrayList<TaskRecord>();
        long after = 0;
        List<TaskRecord> page;
        do {
            page = store.tasks(TaskState.QUEUED, queue, after, 400);
            seen.addAll(page);
            after = page.isEmpty() ? after : page.getLast().id();
        } while (!page.isEmpty());

        assertThat(seen).hasSize(1100);
        assertThat(seen.stream().map(TaskRecord::id).distinct().count()).isEqualTo(1100);
        assertThat(seen)
                .isSortedAccordingTo(
                        java.util.Comparator.comparing(TaskRecord::due).thenComparingLong(TaskRecord::id));
    }

    @Test
    void numericIdOrderBreaksTiesBetweenEqualScores() {
        for (var id : new long[] {9, 10, 100, 11}) {
            enqueued(id);
            jedis.zadd(keys.queue(queue), 5_000, String.valueOf(id));
            jedis.hset(keys.task(id), "due", "5000");
        }

        var first = store.tasks(TaskState.QUEUED, queue, 0, 2);
        var second = store.tasks(TaskState.QUEUED, queue, first.getLast().id(), 10);

        assertThat(first).extracting(TaskRecord::id).containsExactly(9L, 10L);
        assertThat(second).extracting(TaskRecord::id).containsExactly(11L, 100L);
    }

    @Test
    void queuedScheduledAndLeasedListingsFilterByState() {
        var other = store.queueId("other");
        store.enqueue(task(1).build());
        store.enqueue(task(2).due(Instant.now().plusSeconds(60)).build());
        store.enqueue(task(3).queueId(other).due(Instant.now().plusSeconds(90)).build());
        store.enqueue(task(4).build());
        claim(1);

        assertThat(store.tasks(TaskState.QUEUED, null, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactly(4L);
        assertThat(store.tasks(TaskState.LEASED, queue, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactly(1L);
        assertThat(store.tasks(TaskState.SCHEDULED, null, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactly(2L, 3L);
        assertThat(store.tasks(TaskState.SCHEDULED, other, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactly(3L);
        assertThat(store.tasks(TaskState.SCHEDULED, null, 2, 10))
                .extracting(TaskRecord::id)
                .containsExactly(3L);
    }

    // ---- schedules -----------------------------------------------------------------------------------

    private RecurringDefinition definition(String id, String cron) {
        return new RecurringDefinition(id, event, 2, queue, "acme", cron, "{\"d\":1}", 5, 0);
    }

    @Test
    void recurringDefinitionsAreStoredListedAndDeleted() {
        var fire = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);

        store.putRecurring(definition("s", "* * * * *"), fire);

        assertThat(store.recurringSchedules()).containsEntry("s", fire);
        assertThat(store.dueRecurring(fire.minusSeconds(1), 10)).isEmpty();
        var due = store.dueRecurring(fire, 10).getFirst();
        assertThat(due.scheduleId()).isEqualTo("s");
        assertThat(due.fireAt()).isEqualTo(fire);
        var d = due.definition();
        assertThat(d.eventId()).isEqualTo(event);
        assertThat(d.queueId()).isEqualTo(queue);
        assertThat(d.payloadVersion()).isEqualTo(2);
        assertThat(d.account()).isEqualTo("acme");
        assertThat(d.cron()).isEqualTo("* * * * *");
        assertThat(d.data()).isEqualTo("{\"d\":1}");
        assertThat(d.maxAttempts()).isEqualTo(5);
        assertThat(d.revision()).isPositive();
        assertThat(store.deleteRecurring("s")).isTrue();
        assertThat(store.deleteRecurring("s")).isFalse();
        assertThat(store.recurringSchedules()).isEmpty();
        assertThat(jedis.exists(keys.cronDefinition("s"))).isFalse();
    }

    @Test
    void replacingADefinitionKeepsTheFireTimeOnlyWhenTheCronIsUnchanged() {
        var first = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        var second = first.plusSeconds(500);
        store.putRecurring(definition("s", "* * * * *"), first);
        var rev1 = store.dueRecurring(first, 1).getFirst().definition().revision();

        store.putRecurring(definition("s", "* * * * *"), second);
        assertThat(store.recurringSchedules()).containsEntry("s", first);
        var rev2 = store.dueRecurring(first, 1).getFirst().definition().revision();
        store.putRecurring(definition("s", "0 * * * *"), second);

        assertThat(rev2).isGreaterThan(rev1);
        assertThat(store.recurringSchedules()).containsEntry("s", second);
    }

    @Test
    void commitStoresTheOccurrenceScheduledAndAdvancesTheDefinitionOnce() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        var next = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        store.putRecurring(definition("s", "* * * * *"), fire);
        var due = store.dueRecurring(Instant.now(), 10).getFirst();
        var occurrence = task(50).scheduleId("s").due(fire).build();

        assertThat(store.commitRecurring(due, next, occurrence)).isTrue();

        var stored = store.getTask(50).orElseThrow();
        assertThat(stored.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(stored.due()).isEqualTo(fire);
        assertThat(stored.scheduleId()).isEqualTo("s");
        assertThat(store.recurringSchedules()).containsEntry("s", next);
        assertThat(jedis.zscore(keys.scheduled, "50")).isEqualTo((double) fire.toEpochMilli());
        assertThat(store.commitRecurring(due, next.plusSeconds(60), task(51).build()))
                .isFalse();
        assertThat(store.getTask(51)).isEmpty();
        assertThat(store.promote(10).moved()).isEqualTo(1);
    }

    @Test
    void commitIsRejectedForAReplacedOrDeletedDefinition() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        store.putRecurring(definition("s", "* * * * *"), fire);
        var due = store.dueRecurring(Instant.now(), 10).getFirst();
        store.putRecurring(definition("s", "* * * * *"), fire);

        assertThat(store.commitRecurring(due, fire.plusSeconds(60), task(60).build()))
                .isFalse();

        var fresh = store.dueRecurring(Instant.now(), 10).getFirst();
        store.deleteRecurring("s");
        assertThat(store.commitRecurring(fresh, fire.plusSeconds(60), task(61).build()))
                .isFalse();
        assertThat(store.getTask(60)).isEmpty();
        assertThat(store.getTask(61)).isEmpty();
    }

    @Test
    void storeFailuresSurfaceAsTaskStoreException() {
        try (var dead = RedisClient.builder()
                .hostAndPort(new HostAndPort("localhost", 1))
                .build()) {
            var broken = new RedisTaskStore(dead, "x");

            assertThatThrownBy(() -> broken.allocateIds(1)).isInstanceOf(TaskStoreException.class);
        }
    }

    @Test
    void namespaceIsValidated() {
        assertThatThrownBy(() -> new RedisTaskStore(null, "ok")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisTaskStore(jedis, " ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisTaskStore(jedis, "a{b")).isInstanceOf(IllegalArgumentException.class);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
