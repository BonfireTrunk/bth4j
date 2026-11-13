package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.providers.PooledConnectionProvider;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.Loggers;
import today.bonfire.oss.bth4j.PutScheduleOutcome;
import today.bonfire.oss.bth4j.PutScheduleResult;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.ExpiredTask;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.LeaseStatus;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.RecordJson;
import today.bonfire.oss.bth4j.spi.StoredSchedule;
import today.bonfire.oss.bth4j.spi.TaskRecord;

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
    private String namespace;
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
        namespace = "T" + NAMESPACES.incrementAndGet();
        store = RedisTaskStore.builder().client(jedis).namespace(namespace).build();
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

    private TaskRecord enqueued(long id) {
        assertThat(store.enqueue(task(id).build())).isEqualTo(EnqueueOutcome.STORED);
        return store.task(id).orElseThrow();
    }

    private TaskRecord claimOne(long id) {
        enqueued(id);
        return claim(1).getFirst();
    }

    private List<TaskRecord> claim(int count) {
        return store.claim(new ClaimRequest(List.of(new QueueGrant(queue, count, count)), count, LEASE, 1, null));
    }

    private static FailureInfo failure(int attempt) {
        return new FailureInfo(
                attempt, Instant.now().truncatedTo(ChronoUnit.MILLIS), "java.lang.Boom", "boom " + attempt, null);
    }

    private StateCounts counts() {
        return store.counts().getOrDefault(queue, StateCounts.ZERO);
    }

    // ---- identity ------------------------------------------------------------------------------------

    private Set<Integer> queuedTypes() {
        return store.queuedByType().getOrDefault(queue, Map.of()).keySet();
    }

    @Test
    void initWritesSchemaAndRefusesAnotherVersion() {
        assertThat(store.info().engine()).isEqualTo("redis");
        assertThat(jedis.hget(keys.meta, "schema")).isEqualTo(String.valueOf(RedisTaskStore.SCHEMA_VERSION));
        store.init();

        jedis.hset(keys.meta, "schema", "99");

        assertThatThrownBy(store::init)
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("schema version 99, expected " + RedisTaskStore.SCHEMA_VERSION);
        assertThat(jedis.hget(keys.meta, "schema")).isEqualTo("99");
    }

    @Test
    void initRefusesAServerWithoutHashFieldExpiryAndWritesNothing() {
        var connection = new Connection(
                new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)),
                DefaultJedisClientConfig.builder().build());
        try (var old = new UnifiedJedis(connection) {
            @Override
            public List<Long> hpexpireAt(String key, long unixTimeMillis, String... fields) {
                throw new JedisDataException(
                        "ERR unknown command 'HPEXPIREAT', with args beginning with: 'k' '1' 'FIELDS' '1' 'f'");
            }
        }) {
            var other = RedisTaskStore.builder()
                    .client(old)
                    .namespace("T" + NAMESPACES.incrementAndGet())
                    .build();

            assertThatThrownBy(other::init)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("Redis 7.4 or newer, or Valkey 9 or newer");
            assertThat(old.exists(other.keys().meta)).isFalse();
        }
    }

    @Test
    void aListingLeavesOutATaskThatLeftTheStateBetweenTheReadOfTheIdsAndOfTheRecords() {
        enqueued(1);
        enqueued(2);
        var provider = new PooledConnectionProvider(
                new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)),
                DefaultJedisClientConfig.builder().resp3().build());
        var raced = new AtomicBoolean();
        try (var racing = new UnifiedJedis(provider, RedisProtocol.RESP3) {
            // the listing reads the ids with plain calls and the records through its first pipeline
            @Override
            public AbstractPipeline pipelined() {
                if (raced.compareAndSet(false, true)) claim(1);
                return super.pipelined();
            }
        }) {
            var page = RedisTaskStore.builder()
                    .client(racing)
                    .namespace(namespace)
                    .build()
                    .tasks(TaskState.QUEUED, queue, null, 10);

            assertThat(raced).isTrue();
            assertThat(page.items())
                    .hasSize(1)
                    .allSatisfy(t -> assertThat(t.state()).isEqualTo(TaskState.QUEUED));
        }
    }

    @Test
    void initRefusesAServerThatCanEvictKeys() {
        jedis.configSet("maxmemory-policy", "volatile-lru");
        try {
            assertThatThrownBy(store::init)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("maxmemory-policy is volatile-lru and this store needs noeviction");
        } finally {
            jedis.configSet("maxmemory-policy", "noeviction");
        }
        store.init();
    }

    @Test
    void initWarnsAndGoesOnWhenTheUserMayNotRunInfo() {
        var password = UUID.randomUUID().toString();
        jedis.sendCommand(
                Protocol.Command.ACL, "SETUSER", "no-info", "on", ">" + password, "~*", "&*", "+@all", "-info");
        var config = DefaultJedisClientConfig.builder()
                .user("no-info")
                .password(password)
                .build();
        try (var limited = RedisClient.builder()
                .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                .clientConfig(config)
                .build()) {
            var other = RedisTaskStore.builder()
                    .client(limited)
                    .namespace("T" + NAMESPACES.incrementAndGet())
                    .build();

            assertThat(storeLog(other::init)).singleElement().satisfies(e -> {
                assertThat(e.getLevel()).isEqualTo(Level.WARN);
                assertThat(e.getFormattedMessage())
                        .contains("Redis refused INFO memory (NOPERM")
                        .contains("did not check that maxmemory-policy is noeviction");
            });
            assertThat(limited.hget(other.keys().meta, "schema"))
                    .isEqualTo(String.valueOf(RedisTaskStore.SCHEMA_VERSION));
        } finally {
            jedis.sendCommand(Protocol.Command.ACL, "DELUSER", "no-info");
        }
    }

    @Test
    void initWarnsAndGoesOnWhenInfoHasNoEvictionPolicy() {
        var connection = new Connection(
                new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)),
                DefaultJedisClientConfig.builder().build());
        try (var proxy = new UnifiedJedis(connection) {
            @Override
            public String info(String section) {
                return "# Memory\r\nused_memory:1024\r\n";
            }
        }) {
            var other = RedisTaskStore.builder()
                    .client(proxy)
                    .namespace("T" + NAMESPACES.incrementAndGet())
                    .build();

            assertThat(storeLog(other::init)).singleElement().satisfies(e -> {
                assertThat(e.getLevel()).isEqualTo(Level.WARN);
                assertThat(e.getFormattedMessage())
                        .contains("Redis INFO memory has no maxmemory_policy, so init did not check");
            });
            assertThat(proxy.hget(other.keys().meta, "schema"))
                    .isEqualTo(String.valueOf(RedisTaskStore.SCHEMA_VERSION));
        }
    }

    /** Runs {@code action} and returns what it logged to {@link Loggers#REDIS_STORE}. */
    private static List<ILoggingEvent> storeLog(Runnable action) {
        var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Loggers.REDIS_STORE);
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        try {
            action.run();
        } finally {
            logger.detachAppender(logs);
        }
        return logs.list;
    }

    @Test
    void idsAreReservedInBlocks() {
        assertThat(store.allocateIds(10)).isEqualTo(1);
        assertThat(store.allocateIds(5)).isEqualTo(11);
        assertThat(store.allocateIds(1)).isEqualTo(16);
        assertThatThrownBy(() -> store.allocateIds(0)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void runnersGetDistinctIdsAndAreRecorded() {
        var a = store.allocateRunnerId("host-a:1", Set.of(), Set.of());
        var b = store.allocateRunnerId("host-b:2", Set.of(), Set.of());

        assertThat(a).isNotEqualTo(b);
        assertThat(jedis.hget(keys.runner(a), "label")).isEqualTo("host-a:1");
        assertThat(jedis.hget(keys.runner(a), "started")).isNotNull();
    }

    @Test
    void registriesAreStableAndListed() {
        var other = store.queueId("other");

        assertThat(store.queueId("default")).isEqualTo(queue);
        assertThat(other).isNotEqualTo(queue);
        assertThat(RedisTaskStore.builder()
                        .client(jedis)
                        .namespace(keys.namespace)
                        .build()
                        .queueId("other"))
                .isEqualTo(other);
        assertThat(store.queueNames()).containsEntry(queue, "default").containsEntry(other, "other");
        assertThat(store.typeNames()).containsEntry(type, "send");
        assertThat(jedis.hvals(keys.registry("queue"))).contains(String.valueOf(queue), String.valueOf(other));
    }

    @Test
    void registryRefusesMoreThan32767Names() {
        jedis.set(keys.prefix + "seq:type", "32767");

        assertThatThrownBy(() -> store.typeId("one too many")).isInstanceOf(TaskStoreException.class);
    }

    // ---- enqueue -------------------------------------------------------------------------------------

    @Test
    void enqueueStoresOnlyNonNullFieldsWithoutExpiry() {
        enqueued(1);

        var hash = jedis.hgetAll(keys.task(1));

        assertThat(hash.keySet())
                .containsExactlyInAnyOrder("rev", "q", "ty", "pv", "state", "att", "max", "created", "due", "sc", "wt");
        assertThat(hash).containsEntry("state", "0");
        assertThat(jedis.ttl(keys.task(1))).isEqualTo(-1);
        assertThat(jedis.zscore(keys.queue(queue, type), "1")).isEqualTo(Double.valueOf(hash.get("due")));
        assertThat(counts().queued()).isEqualTo(1);
    }

    @Test
    void enqueueResendWithTheSameTokenIsStoredAndAnotherTokenExists() {
        var scripts = new ScriptRunner(jedis, keys.meta, keys.prefix);
        for (long id : List.of(11L, 12L)) {
            var record = task(id).uniqueKey(id == 11 ? null : "resend-key").build();
            var args = RedisTaskStore.enqueueArgs(record);

            assertThat(scripts.run(RedisScripts.ENQUEUE, args)).isEqualTo(List.of(1L, ""));
            var before = jedis.hgetAll(keys.task(id));
            assertThat(scripts.run(RedisScripts.ENQUEUE, args)).isEqualTo(List.of(1L, ""));
            assertThat(scripts.run(RedisScripts.ENQUEUE, RedisTaskStore.enqueueArgs(record)))
                    .isEqualTo(List.of(0L, ""));
            assertThat(jedis.hgetAll(keys.task(id))).isEqualTo(before);
            assertThat(jedis.zrange(keys.queue(queue, type), 0, -1)).contains(String.valueOf(id));
        }
        assertThat(counts().queued()).isEqualTo(2);
    }

    @Test
    void enqueueCallsAndBatchRecordsHaveDistinctWriteTokens() {
        var first = task(21).build();
        var second = task(22).build();
        assertThat(store.enqueue(first)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.enqueueAll(List.of(first, second, task(23).build())))
                .containsExactly(EnqueueOutcome.EXISTS, EnqueueOutcome.STORED, EnqueueOutcome.STORED);
        assertThat(List.of(21L, 22L, 23L).stream()
                        .map(id -> jedis.hget(keys.task(id), "wt"))
                        .toList())
                .doesNotContainNull()
                .doesNotHaveDuplicates();
        assertThat(counts().queued()).isEqualTo(3);
    }

    @Test
    void enqueueRoundTripsEveryField() {
        var t = task(7).encodedPayload("{\"a\":1}")
                .account("acme")
                .headers(Map.of("h", "v"))
                .retryPolicy("slow")
                .scheduleId("sched")
                .retryOf(3L)
                .startBy(Instant.ofEpochMilli(1_900_000_000_000L))
                .maxRunTime(Duration.ofMinutes(2))
                .payloadVersion(4)
                .build();

        store.enqueue(t);
        var back = store.task(7).orElseThrow();

        assertThat(back.encodedPayload()).isEqualTo("{\"a\":1}");
        assertThat(back.account()).isEqualTo("acme");
        assertThat(back.headers()).containsEntry("h", "v");
        assertThat(back.retryPolicy()).isEqualTo("slow");
        assertThat(back.scheduleId()).isEqualTo("sched");
        assertThat(back.retryOf()).isEqualTo(3L);
        assertThat(back.startBy()).isEqualTo(t.startBy());
        assertThat(back.maxRunTime()).isEqualTo(Duration.ofMinutes(2));
        assertThat(back.payloadVersion()).isEqualTo(4);
        assertThat(back.createdAt()).isEqualTo(t.createdAt());
        assertThat(back.state()).isEqualTo(TaskState.QUEUED);
        assertThat(back.dueAt()).isEqualTo(back.stateChangedAt());
    }

    @Test
    void futureDueIsScheduledAndPastDueIsQueued() {
        var future = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        store.enqueue(task(1).dueAt(future).build());
        store.enqueue(task(2).dueAt(Instant.now().minusSeconds(60)).build());

        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.task(1).orElseThrow().dueAt()).isEqualTo(future);
        assertThat(store.task(2).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(jedis.zscore(keys.scheduled(queue), "1")).isEqualTo((double) future.toEpochMilli());
        assertThat(store.nextDue()).isEqualTo(future);
        assertThat(counts().scheduled()).isEqualTo(1);
    }

    @Test
    void existingIdIsReportedInAnyState() {
        var t = claimOne(1);
        assertThat(store.enqueue(task(1).build())).isEqualTo(EnqueueOutcome.EXISTS);

        store.ack(1, t.revision(), true, true, null);

        assertThat(store.enqueue(task(1).build())).isEqualTo(EnqueueOutcome.EXISTS);
        assertThat(counts().queued()).isZero();
    }

    @Test
    void duplicateUniqueKeyNamesTheHolder() {
        store.enqueue(task(1).uniqueKey("k").build());

        var outcome = store.enqueue(task(2).uniqueKey("k").build());

        assertThat(outcome).isEqualTo(new EnqueueOutcome.DuplicateKey(1));
        assertThat(store.task(2)).isEmpty();
        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("1");
    }

    @Test
    void uniqueKeyIsReleasedWhenTheTaskEnds() {
        store.enqueue(task(1).uniqueKey("k").build());
        var t = claim(1).getFirst();
        assertThat(store.enqueue(task(2).uniqueKey("k").build())).isInstanceOf(EnqueueOutcome.DuplicateKey.class);

        store.ack(1, t.revision(), true, true, null);

        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
        assertThat(jedis.hexists(keys.task(1), "uk")).isFalse();
        assertThat(store.enqueue(task(2).uniqueKey("k").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void keyKeepWindowHoldsTheKeyViaPexpireAndOutlivesTheHashWithoutKeep() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)).build());
        store.enqueue(task(2).encodedPayload("p")
                .uniqueKey("j")
                .keyKeep(Duration.ofMinutes(5))
                .build());
        var claimed = claim(2);

        store.ack(1, claimed.get(0).revision(), true, true, null);
        store.ack(2, claimed.get(1).revision(), false, false, null);

        assertThat(jedis.pttl(keys.uniqueKey("k")))
                .isBetween(1L, Duration.ofMinutes(5).toMillis());
        assertThat(jedis.pttl(keys.uniqueKey("j")))
                .isBetween(1L, Duration.ofMinutes(5).toMillis());
        assertThat(store.task(1).orElseThrow().keyUntil()).isNotNull();
        assertThat(store.task(2)).isEmpty();
        assertThat(jedis.exists(keys.task(2), keys.data(2))).isZero();
        assertThat(jedis.get(keys.uniqueKey("j"))).isEqualTo("2");
        assertThat(store.enqueue(task(3).uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(1));
        assertThat(store.enqueue(task(4).uniqueKey("j").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(2));
        jedis.pexpire(keys.uniqueKey("k"), 1);
        awaitExpiry(() -> !jedis.exists(keys.uniqueKey("k")));
        assertThat(store.enqueue(task(3).uniqueKey("k").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void keyFieldsOfFinishedTasksExpireWithTheWindowAndReleaseKeysHasNothingToDo() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)).build());
        var t = claim(1).getFirst();
        store.ack(1, t.revision(), true, true, null);
        assertThat(jedis.hpttl(keys.task(1), "uk", "kk", "ku"))
                .allSatisfy(ttl -> assertThat(ttl).isPositive());
        assertThat(store.task(1).orElseThrow().uniqueKey()).isEqualTo("k");
        assertThat(jedis.exists(keys.uniqueKey("k"))).isTrue();
        assertThat(store.releaseKeys(10)).isZero();

        jedis.hpexpire(keys.task(1), 1, "uk", "kk", "ku");
        jedis.pexpire(keys.uniqueKey("k"), 1);
        awaitExpiry(() -> !jedis.hexists(keys.task(1), "uk") && !jedis.exists(keys.uniqueKey("k")));

        var back = store.task(1).orElseThrow();
        assertThat(back.uniqueKey()).isNull();
        assertThat(back.keyKeep()).isNull();
        assertThat(back.keyUntil()).isNull();
        assertThat(jedis.hgetAll(keys.task(1))).doesNotContainKeys("uk", "kk", "ku");
        assertThat(jedis.exists(keys.uniqueKey("k"))).isFalse();
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
        store.enqueue(task(1).dueAt(now.plusSeconds(10)).build());
        store.enqueue(task(2).dueAt(now).build());
        store.enqueue(task(3).dueAt(now.plusSeconds(20)).build());
        var before = store.task(1).orElseThrow();

        var claimed = claim(2);

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(2L, 1L);
        var first = claimed.get(1);
        assertThat(first.state()).isEqualTo(TaskState.LEASED);
        assertThat(first.revision()).isEqualTo(before.revision() + 1);
        assertThat(first.attempts()).isEqualTo(1);
        assertThat(first.runnerId()).isEqualTo(1);
        assertThat(first.claimedAt()).isNotNull();
        assertThat(first.leaseUntil()).isAfter(Instant.now().plusSeconds(25));
        assertThat(first.dueAt()).isEqualTo(before.dueAt());
        assertThat(jedis.zscore(keys.leased, "1"))
                .isEqualTo((double) first.leaseUntil().toEpochMilli());
        assertThat(jedis.zscore(keys.index(2, queue), "2")).isNotNull();
        assertThat(jedis.zscore(keys.queue(queue, type), "1")).isNull();
        assertThat(counts().queued()).isEqualTo(1);
        assertThat(counts().leased()).isEqualTo(2);
    }

    @Test
    void aClaimLeavesOutATaskItCannotReadAndEndsItsLeaseSoMaintenanceCanEndIt() {
        var now = Instant.now().minusSeconds(100);
        store.enqueue(task(1).dueAt(now).build());
        store.enqueue(task(2).dueAt(now.plusSeconds(1)).build());
        store.enqueue(task(3).dueAt(now.plusSeconds(2)).build());
        jedis.hset(keys.task(2), Keys.F_HEADERS, "{\"tenant\":42}");

        var claimed = claim(3);

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(1L, 3L);
        assertThatThrownBy(() -> store.task(2))
                .isInstanceOfSatisfying(
                        UnreadableTaskException.class,
                        e -> assertThat(e.taskId()).isEqualTo(2))
                .hasMessage("task 2 has a stored value that cannot be read");
        var expired = store.reclaim(10);
        assertThat(expired).extracting(ExpiredLease::id).containsExactly(2L);
        assertThat(store.failExpired(2, expired.getFirst().revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(counts().dead()).isEqualTo(1);
        assertThat(counts().leased()).isEqualTo(2);
    }

    @Test
    void aClaimLeasesATaskWhoseNumbersCannotBeReadAndStillLeasesTheOthersInTheSameRun() {
        var now = Instant.now().minusSeconds(100);
        store.enqueue(task(1).dueAt(now).build());
        store.enqueue(task(2).dueAt(now.plusSeconds(1)).build());
        store.enqueue(task(3).dueAt(now.plusSeconds(2)).build());
        jedis.hset(keys.task(2), Map.of("rev", "x", "att", "y", "sb", "z"));

        var claimed = claim(3);

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(1L, 3L);
        assertThat(jedis.hget(keys.task(2), "state")).isEqualTo("2");
        assertThat(jedis.zscore(keys.queue(queue, type), "2")).isNull();
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(2L);
        assertThatThrownBy(() -> store.task(2)).isInstanceOf(UnreadableTaskException.class);
        assertThat(counts().leased()).isEqualTo(3);
    }

    @Test
    void reclaimReturnsALeaseWithUnreadableCountsAndRepairsAnUnreadableRevisionForTheNextRound() {
        for (var id = 1L; id <= 3; id++) {
            store.enqueue(task(id).build());
        }
        claim(3);
        for (var id = 1L; id <= 3; id++) {
            expireLease(id);
        }
        jedis.hset(keys.task(1), "att", "x");
        jedis.hset(keys.task(2), "rev", "y");
        var revision = Integer.parseInt(jedis.hget(keys.task(1), "rev"));

        var expired = store.reclaim(10);

        assertThat(expired)
                .extracting(ExpiredLease::id, ExpiredLease::revision, ExpiredLease::attempts)
                .containsExactlyInAnyOrder(tuple(1L, revision, 0), tuple(3L, revision, 1));
        assertThat(jedis.zscore(keys.leased, "2")).isNotNull();
        assertThat(jedis.hget(keys.task(2), "rev")).isEqualTo("0");
        assertThat(store.reclaim(10))
                .extracting(ExpiredLease::id, ExpiredLease::revision)
                .containsExactlyInAnyOrder(tuple(1L, revision), tuple(2L, 0), tuple(3L, revision));
        assertThat(store.failExpired(2, 0, failure(1), null, true, null)).isEqualTo(FailOutcome.DEAD);
    }

    @Test
    void reclaimAndExpireReportZeroForAQueueOrTypeIdThatCannotBeRead() {
        var startBy = Instant.now().minusSeconds(1);
        for (var id = 1L; id <= 2; id++) {
            store.enqueue(task(id).build());
        }
        claim(2);
        for (var id = 1L; id <= 2; id++) {
            expireLease(id);
        }
        store.enqueue(task(3).startBy(startBy).build());
        store.enqueue(task(4).startBy(startBy).build());
        store.enqueue(task(5).startBy(startBy).build());
        jedis.hset(keys.task(1), "q", "x");
        jedis.hset(keys.task(2), "ty", "y");
        jedis.hset(keys.task(3), "q", "x");
        jedis.hset(keys.task(4), "ty", "y");
        jedis.hset(keys.task(5), "q", "1.5");

        assertThat(store.reclaim(10))
                .extracting(ExpiredLease::id, ExpiredLease::queueId, ExpiredLease::typeId)
                .containsExactlyInAnyOrder(tuple(1L, 0, type), tuple(2L, queue, 0));
        assertThat(store.expire(10))
                .containsExactlyInAnyOrder(
                        new ExpiredTask(3, 0, type), new ExpiredTask(4, queue, 0), new ExpiredTask(5, 0, type));
    }

    @Test
    void promoteMovesATaskWithAnUnreadableDueOrRevisionAndStillPromotesTheOthers() {
        for (var id = 1L; id <= 3; id++) {
            store.enqueue(task(id).dueAt(Instant.now().plusSeconds(3600)).build());
            assertThat(jedis.hget(keys.task(id), "state")).isEqualTo("1");
            jedis.zadd(keys.scheduled(queue), 1, String.valueOf(id));
        }
        jedis.hset(keys.task(1), "due", "x");
        jedis.hset(keys.task(2), "rev", "y");

        store.promote(10);

        for (var id = 1L; id <= 3; id++) {
            assertThat(jedis.hget(keys.task(id), "state")).isEqualTo("0");
            assertThat(jedis.zscore(keys.queue(queue, type), String.valueOf(id)))
                    .isNotNull();
        }
        assertThat(jedis.hget(keys.task(2), "rev")).isEqualTo("1");
        assertThat(jedis.zcard(keys.scheduled(queue))).isZero();
    }

    @Test
    void expireAndDeleteHandleUnreadableFieldsWithoutStrandingOtherTasks() {
        var startBy = Instant.now().minusSeconds(1);
        for (var id = 1L; id <= 3; id++) {
            store.enqueue(task(id).startBy(startBy)
                    .uniqueKey("k" + id)
                    .keyKeep(Duration.ofMinutes(1))
                    .build());
        }
        jedis.hset(keys.task(1), "rev", "x");
        jedis.hset(keys.task(2), "kk", "y");
        jedis.hset(keys.task(3), "state", "z");

        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactlyInAnyOrder(1L, 2L);
        assertThat(jedis.zscore(keys.index(6, queue), "1")).isNotNull();
        assertThat(jedis.zscore(keys.index(6, queue), "2")).isNotNull();
        assertThat(jedis.pttl(keys.uniqueKey("k1"))).isPositive();
        assertThat(jedis.exists(keys.uniqueKey("k2")))
                .as("an unreadable keep window counts as none")
                .isFalse();
        assertThat(store.delete(3, Integer.parseInt(jedis.hget(keys.task(3), "rev")), null))
                .isEqualTo(DeleteOutcome.DELETED);
        assertThat(jedis.exists(keys.task(3))).isFalse();
    }

    @Test
    void failEndsATaskWhoseQueueLeaseOrAttemptsCannotBeRead() {
        for (var id = 1L; id <= 3; id++) {
            store.enqueue(task(id).build());
        }
        var revision = claim(3).getFirst().revision();
        jedis.hset(keys.task(1), "q", "x");
        jedis.hset(keys.task(2), "lease", "y");
        jedis.hset(keys.task(3), "att", "z");
        var retryAt = Instant.now().plusSeconds(60);

        assertThat(store.fail(1, revision, failure(1), retryAt, true, null)).isEqualTo(FailOutcome.DEAD);
        assertThat(store.failExpired(2, revision, failure(1), null, true, null)).isEqualTo(FailOutcome.DEAD);
        assertThat(store.fail(3, revision, failure(1), retryAt, true, null)).isEqualTo(FailOutcome.DEAD);

        assertThat(jedis.exists(keys.task(1)))
                .as("a task that no queue can hold is removed")
                .isFalse();
        assertThat(jedis.hget(keys.task(2), "state")).isEqualTo("4");
        assertThat(jedis.hget(keys.task(3), "state")).isEqualTo("4");
        assertThat(jedis.zcard(keys.leased)).isZero();
        assertThat(jedis.zcard(keys.scheduled(queue))).isZero();
        assertThat(counts().dead()).isEqualTo(2);
        assertThat(counts().leased()).as("the removed task is counted nowhere").isZero();
    }

    @Test
    void renewAnswersNotOwnedForALeaseThatCannotBeReadAndStillRenewsTheOthersInTheCall() {
        for (var id = 1L; id <= 3; id++) {
            store.enqueue(task(id).build());
        }
        var revision = claim(3).getFirst().revision();
        jedis.hset(keys.task(2), "lease", "y");

        var statuses = store.renew(
                List.of(new LeaseRef(1, revision), new LeaseRef(2, revision), new LeaseRef(3, revision)),
                Duration.ofMinutes(10));

        assertThat(statuses).extracting(LeaseStatus::owned).containsExactly(true, false, true);
        var renewed = (double) Instant.now().plus(Duration.ofMinutes(9)).toEpochMilli();
        assertThat(jedis.zscore(keys.leased, "1")).isGreaterThan(renewed);
        assertThat(jedis.zscore(keys.leased, "3")).isGreaterThan(renewed);
        assertThat(jedis.zscore(keys.leased, "2")).isLessThan(renewed);
    }

    @Test
    void endingTheLeasesOfUnreadableTasksLeavesTasksOfAnotherClaimAlone() {
        var leased = claimOne(1);
        long claimedAt = leased.claimedAt().toEpochMilli();
        var scripts = new ScriptRunner(jedis, keys.meta, keys.prefix);

        scripts.run(RedisScripts.END_LEASES, List.of(String.valueOf(claimedAt + 1), "1", "1"));
        scripts.run(RedisScripts.END_LEASES, List.of(String.valueOf(claimedAt), "2", "1"));

        assertThat(store.reclaim(10)).isEmpty();
        assertThat(jedis.zscore(keys.leased, "1"))
                .isEqualTo((double) leased.leaseUntil().toEpochMilli());
    }

    @Test
    void claimReadsQueuesInTheGivenOrderOnlyAsNeeded() {
        var other = store.queueId("other");
        store.enqueue(task(1).build());
        store.enqueue(task(2).queueId(other).build());
        store.enqueue(task(3).queueId(other).build());

        var one = store.claim(
                new ClaimRequest(List.of(new QueueGrant(other, 1, 1), new QueueGrant(queue, 1, 1)), 1, LEASE, 1, null));
        var rest = store.claim(
                new ClaimRequest(List.of(new QueueGrant(other, 5, 5), new QueueGrant(queue, 5, 5)), 5, LEASE, 1, null));

        assertThat(one).extracting(TaskRecord::id).containsExactly(2L);
        assertThat(rest).extracting(TaskRecord::id).containsExactly(3L, 1L);
        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 0, 0)), 0, LEASE, 1, null)))
                .isEmpty();
    }

    @Test
    void unknownTypesAreUntouchedAndAPassedStartByIsLeftToMaintenance() {
        var unknown = store.typeId("unknown");
        store.enqueue(task(1).typeId(unknown).startBy(Instant.EPOCH).build());
        store.enqueue(task(2).typeId(unknown).build());
        store.enqueue(task(3).build());
        var hashes = List.of(jedis.hgetAll(keys.task(1)), jedis.hgetAll(keys.task(2)));
        var scores = jedis.zrangeWithScores(keys.queue(queue, unknown), 0, -1);
        var startBy = jedis.zscore(keys.startBy, "1");
        var request = new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, 1, Set.of(type));

        assertThat(store.claim(request)).extracting(TaskRecord::id).containsExactly(3L);
        assertThat(store.claim(request)).isEmpty();

        assertThat(jedis.hgetAll(keys.task(1))).isEqualTo(hashes.get(0));
        assertThat(jedis.hgetAll(keys.task(2))).isEqualTo(hashes.get(1));
        assertThat(jedis.zrangeWithScores(keys.queue(queue, unknown), 0, -1)).isEqualTo(scores);
        assertThat(jedis.zscore(keys.startBy, "1")).isEqualTo(startBy);
        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, 1, Set.of())))
                .isEmpty();
        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, 1, Set.of(unknown))))
                .extracting(TaskRecord::id)
                .containsExactly(2L);
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(jedis.zscore(keys.startBy, "1")).isEqualTo(startBy);
    }

    @Test
    void manyForeignTasksDoNotConsumeTheClaimScanBudget() {
        var foreign = store.typeId("foreign");
        var tasks = new ArrayList<TaskRecord>();
        for (var id = 1; id <= 1500; id++)
            tasks.add(task(id).typeId(foreign).dueAt(Instant.EPOCH).build());
        tasks.add(task(2000).dueAt(Instant.ofEpochMilli(1)).build());
        store.enqueueAll(tasks);
        var before = jedis.zrangeWithScores(keys.queue(queue, foreign), 0, -1);

        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, Set.of(type))))
                .extracting(TaskRecord::id)
                .containsExactly(2000L);

        assertThat(jedis.zrangeWithScores(keys.queue(queue, foreign), 0, -1)).isEqualTo(before);
        assertThat(store.queuedByType()).isEqualTo(Map.of(queue, Map.of(foreign, 1500L)));
        assertThat(queuedTypes()).containsExactly(foreign);
    }

    @Test
    void activeTypesFollowTheTypeQueuesThatHoldTasks() {
        var second = store.typeId("second");
        store.enqueue(task(1).build());
        store.enqueue(task(2).typeId(second).build());
        assertThat(jedis.smembers(keys.activeTypes(queue)))
                .containsExactlyInAnyOrder(String.valueOf(type), String.valueOf(second));

        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, 1, Set.of(second))))
                .extracting(TaskRecord::id)
                .containsExactly(2L);
        assertThat(jedis.smembers(keys.activeTypes(queue))).containsExactly(String.valueOf(type));

        var t = claim(5).getFirst();
        assertThat(jedis.exists(keys.activeTypes(queue))).isFalse();
        assertThat(store.unclaim(t.id(), t.revision())).isTrue();
        assertThat(jedis.smembers(keys.activeTypes(queue))).containsExactly(String.valueOf(type));
    }

    @Test
    void aClaimWritesAMissingTypeSetAndEqualTypeSetsShareIt() {
        var known = Set.of(type, store.typeId("idle"));
        store.enqueue(task(1).build());

        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known)))
                .extracting(TaskRecord::id)
                .containsExactly(1L);
        var sets = jedis.keys(keys.prefix + "ts:*");
        assertThat(sets).hasSize(1);
        var set = sets.iterator().next();
        assertThat(jedis.smembers(set))
                .containsExactlyInAnyOrder(
                        String.valueOf(type),
                        String.valueOf(known.stream()
                                .filter(id -> id != type)
                                .findFirst()
                                .orElseThrow()));
        assertThat(jedis.pttl(set)).isPositive();

        jedis.del(set);
        store.enqueue(task(2).build());
        assertThat(store.claim(
                        new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 2, new HashSet<>(known))))
                .extracting(TaskRecord::id)
                .containsExactly(2L);
        assertThat(jedis.keys(keys.prefix + "ts:*")).containsExactly(set);
    }

    @Test
    void aClaimReadsOnlyTheTypesThatHaveQueuedTasks() {
        var known = new HashSet<Integer>();
        known.add(type);
        for (var i = 0; i < 2_000; i++) known.add(store.typeId("idle-" + i));
        store.enqueue(task(1).build());
        store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, 1, known));
        store.enqueue(task(2).build());
        var before = zrangeCalls();

        assertThat(store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, 1, known)))
                .extracting(TaskRecord::id)
                .containsExactly(2L);
        assertThat(zrangeCalls() - before).isLessThanOrEqualTo(2);
    }

    private long zrangeCalls() {
        var stats = jedis.info("commandstats");
        var at = stats.indexOf("cmdstat_zrange:calls=");
        if (at < 0) return 0;
        var from = at + "cmdstat_zrange:calls=".length();
        return Long.parseLong(stats.substring(from, stats.indexOf(',', from)));
    }

    @Test
    void claimMergesTypeQueuesByDueThenLexicalIdAcrossBothPasses() {
        var types = List.of(type, store.typeId("second"), store.typeId("third"));
        var tasks = new ArrayList<TaskRecord>();
        for (var id = 1; id <= 210; id++) {
            tasks.add(task(id).typeId(types.get(id % types.size()))
                    .dueAt(Instant.ofEpochMilli(id % 3))
                    .build());
        }
        tasks.add(task(9007199254740993L)
                .typeId(types.get(1))
                .dueAt(Instant.EPOCH)
                .build());
        store.enqueueAll(tasks);
        var expected = tasks.stream()
                .sorted(java.util.Comparator.comparing(TaskRecord::dueAt).thenComparing(t -> String.valueOf(t.id())))
                .map(TaskRecord::id)
                .toList();

        var claimed = store.claim(
                new ClaimRequest(List.of(new QueueGrant(queue, 2, tasks.size())), tasks.size(), LEASE, 1, null));

        assertThat(claimed).extracting(TaskRecord::id).containsExactlyElementsOf(expected);
        assertThat(queuedTypes()).isEmpty();
        assertThat(jedis.keys(keys.prefix + "q:*")).isEmpty();
    }

    @Test
    void scanBudgetIsSharedAcrossPassesAndCleansStaleEntries() {
        store.enqueue(task(1).dueAt(Instant.EPOCH).build());
        store.enqueue(task(2).dueAt(Instant.ofEpochMilli(2)).build());
        var stale = new java.util.HashMap<String, Double>();
        for (var id = 1000; id < 2100; id++) stale.put(String.valueOf(id), 1d);
        jedis.zadd(keys.queue(queue, type), stale);

        var claimed = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 300)), 300, LEASE, 1, null));

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(1L);
        assertThat(jedis.zcard(keys.queue(queue, type))).isEqualTo(102);
        assertThat(queuedTypes()).containsExactly(type);
        assertThat(claim(300)).extracting(TaskRecord::id).containsExactly(2L);
        assertThat(queuedTypes()).isEmpty();
    }

    @Test
    void queuedListingMergesTypesWithTiesByIdTextAndSurvivesTheCursorTypeDisappearing() {
        var second = store.typeId("second");
        store.enqueue(task(10).dueAt(Instant.EPOCH).build());
        store.enqueue(task(2).typeId(second).dueAt(Instant.EPOCH).build());
        store.enqueue(task(3).typeId(second).dueAt(Instant.ofEpochMilli(1)).build());
        var first = store.tasks(TaskState.QUEUED, queue, null, 1);
        assertThat(first.items()).extracting(TaskRecord::id).containsExactly(10L);
        assertThat(store.delete(10, 0, null)).isEqualTo(DeleteOutcome.DELETED);
        assertThat(queuedTypes()).containsExactly(second);

        var next = store.tasks(TaskState.QUEUED, queue, first.next(), 1);
        assertThat(next.items()).extracting(TaskRecord::id).containsExactly(2L);
        assertThat(store.tasks(TaskState.QUEUED, null, next.next(), 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(3L);
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
                var runner = w;
                calls.add(() -> {
                    var mine = new ArrayList<TaskRecord>();
                    List<TaskRecord> got;
                    do {
                        got = store.claim(
                                new ClaimRequest(List.of(new QueueGrant(queue, 7, 7)), 7, LEASE, runner, null));
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
        var renewed = store.task(1).orElseThrow();
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

        var back = store.task(1).orElseThrow();
        assertThat(back.revision()).isEqualTo(t.revision());
        assertThat(back.steps()).isEqualTo(steps);
        assertThat(back.progress()).isEqualTo("half");
        assertThat(store.progress(1, t.revision(), null)).isTrue();
        assertThat(store.task(1).orElseThrow().progress()).isNull();
        assertThat(store.saveSteps(1, t.revision(), null)).isTrue();
        assertThat(store.task(1).orElseThrow().steps()).isNull();
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

        assertThat(store.ack(1, t.revision(), false, false, null)).isTrue();

        assertThat(store.task(1)).isEmpty();
        assertThat(jedis.zscore(keys.leased, "1")).isNull();
        assertThat(jedis.zscore(keys.index(2, queue), "1")).isNull();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
    }

    private TaskRecord leasedWithEveryKey(long id) {
        assertThat(store.enqueue(
                        task(id).encodedPayload("p").uniqueKey("k" + id).build()))
                .isEqualTo(EnqueueOutcome.STORED);
        var leased = RedisFixtures.leasedWithHistory(store, queue, claim(1).getFirst());
        RedisFixtures.assertPopulated(jedis, keys, id);
        assertThat(jedis.exists(keys.uniqueKey("k" + id))).isTrue();
        return leased;
    }

    @Test
    void ackWithoutKeepRemovesEveryKeyOfTheTask() {
        var t = leasedWithEveryKey(1);

        assertThat(store.ack(1, t.revision(), false, false, null)).isTrue();

        RedisFixtures.assertGone(jedis, keys, 1);
        assertThat(jedis.exists(keys.uniqueKey("k1"))).isFalse();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
    }

    @Test
    void ackWithKeepSucceedsAndDropsDataUnlessKept() {
        store.enqueue(task(1).encodedPayload("payload").build());
        store.enqueue(task(2).encodedPayload("payload").build());
        var claimed = claim(2);
        store.saveSteps(1, claimed.get(0).revision(), Map.of("s", new StepResult(Instant.ofEpochMilli(5), "r")));

        assertThat(store.ack(1, claimed.get(0).revision(), true, false, null)).isTrue();
        assertThat(store.ack(2, claimed.get(1).revision(), true, true, null)).isTrue();

        var dropped = store.task(1).orElseThrow();
        assertThat(dropped.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(dropped.encodedPayload()).isNull();
        assertThat(dropped.steps()).containsKey("s");
        assertThat(dropped.leaseUntil()).isNull();
        assertThat(dropped.runnerId()).isEqualTo(1);
        assertThat(dropped.revision()).isEqualTo(claimed.get(0).revision() + 1);
        assertThat(store.task(2).orElseThrow().encodedPayload()).isEqualTo("payload");
        assertThat(jedis.zscore(keys.index(3, queue), "1"))
                .isEqualTo((double) dropped.stateChangedAt().toEpochMilli());
        assertThat(counts().succeeded()).isEqualTo(2);
        assertThat(jedis.ttl(keys.task(1))).isEqualTo(-1);
    }

    // ---- fail ----------------------------------------------------------------------------------------

    @Test
    void failSchedulesRetryWhileAttemptsRemain() {
        var t = claimOne(1);
        var retryAt = Instant.now().plusSeconds(30).truncatedTo(ChronoUnit.MILLIS);

        assertThat(store.fail(1, t.revision(), failure(1), retryAt, true, null)).isEqualTo(FailOutcome.RETRY);

        var back = store.task(1).orElseThrow();
        assertThat(back.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(back.dueAt()).isEqualTo(retryAt);
        assertThat(back.revision()).isEqualTo(t.revision() + 1);
        assertThat(back.leaseUntil()).isNull();
        assertThat(back.runnerId()).isNull();
        assertThat(back.failures()).hasSize(1);
        assertThat(back.lastFailure().message()).isEqualTo("boom 1");
        assertThat(jedis.zscore(keys.scheduled(queue), "1")).isEqualTo((double) retryAt.toEpochMilli());
        assertThat(jedis.zscore(keys.leased, "1")).isNull();
        assertThat(counts().scheduled()).isEqualTo(1);
        assertThat(counts().leased()).isZero();
    }

    @Test
    void failIsDeadWithoutRetryAtOrWhenAttemptsAreExhausted() {
        var t = claimOne(1);
        assertThat(store.fail(1, t.revision(), failure(1), null, true, null)).isEqualTo(FailOutcome.DEAD);
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.DEAD);
        assertThat(jedis.zscore(keys.index(4, queue), "1")).isNotNull();
        assertThat(counts().dead()).isEqualTo(1);

        store.enqueue(task(2).maxAttempts(1).build());
        var u = claim(1).getFirst();

        assertThat(store.fail(2, u.revision(), failure(1), Instant.now(), true, null))
                .isEqualTo(FailOutcome.DEAD);
    }

    @Test
    void deadTaskIsRemovedWhenNotKept() {
        var t = claimOne(1);

        assertThat(store.fail(1, t.revision(), failure(1), null, false, null)).isEqualTo(FailOutcome.DEAD);

        assertThat(store.task(1)).isEmpty();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
        assertThat(jedis.zscore(keys.index(2, queue), "1")).isNull();
    }

    @Test
    void deadTaskNotKeptRemovesEveryKeyOfTheTask() {
        var t = leasedWithEveryKey(1);

        assertThat(store.fail(1, t.revision(), failure(2), null, false, null)).isEqualTo(FailOutcome.DEAD);

        RedisFixtures.assertGone(jedis, keys, 1);
        assertThat(jedis.exists(keys.uniqueKey("k1"))).isFalse();
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
    }

    @Test
    void deadTaskWithKeyWindowIsRemovedWhenNotKeptAndItsKeyStringStays() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(1)).build());
        var t = claim(1).getFirst();

        assertThat(store.fail(1, t.revision(), failure(1), null, false, null)).isEqualTo(FailOutcome.DEAD);

        assertThat(store.task(1)).isEmpty();
        assertThat(jedis.exists(keys.task(1), keys.data(1), keys.failures(1))).isZero();
        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("1");
        assertThat(jedis.pttl(keys.uniqueKey("k")))
                .isBetween(1L, Duration.ofMinutes(1).toMillis());
        assertThat(jedis.zcard(keys.index(TaskState.DEAD.code(), queue))).isZero();
    }

    @Test
    void deadTaskReleasesItsKeyLikeAnAck() {
        store.enqueue(task(1).uniqueKey("k").build());
        store.enqueue(task(2).uniqueKey("j").keyKeep(Duration.ofMinutes(1)).build());
        var claimed = claim(2);

        store.fail(1, claimed.get(0).revision(), failure(1), null, true, null);
        store.fail(2, claimed.get(1).revision(), failure(1), null, true, null);

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
            assertThat(store.fail(1, t.revision(), f, Instant.now().minusSeconds(1), true, null))
                    .isEqualTo(FailOutcome.RETRY);
            assertThat(store.promote(10).moved()).isEqualTo(1);
        }

        var failures = jedis.lrange(keys.failures(1), 0, -1).stream()
                .map(RecordJson::decodeFailure)
                .toList();

        assertThat(failures).hasSize(20);
        assertThat(failures.getFirst().attempt()).isEqualTo(6);
        assertThat(failures.getLast().attempt()).isEqualTo(25);
        assertThat(failures.getLast().stack()).isEqualTo(nasty);
        assertThat(store.task(1).orElseThrow().failures()).hasSize(20);
    }

    @Test
    void failExpiredIsLostWhileTheLeaseHoldsAndWorksOnceItExpired() {
        enqueued(1);
        var t = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, Duration.ofMinutes(5), 1, null))
                .getFirst();
        assertThat(store.failExpired(1, t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.LOST);
        assertThat(store.reclaim(10)).isEmpty();
        expireLease(1);

        var expired = store.reclaim(10);

        assertThat(expired).hasSize(1);
        var e = expired.getFirst();
        assertThat(e.id()).isEqualTo(1);
        assertThat(e.revision()).isEqualTo(t.revision());
        assertThat(e.attempts()).isEqualTo(1);
        assertThat(e.maxAttempts()).isEqualTo(3);
        assertThat(e.typeId()).isEqualTo(type);
        assertThat(e.cancelRequested()).isFalse();
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(store.failExpired(1, e.revision(), failure(1), Instant.now().plusSeconds(5), true, null))
                .isEqualTo(FailOutcome.RETRY);
    }

    @Test
    void renewedLeaseIsNotReclaimedAsExpired() {
        enqueued(1);
        var t = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, Duration.ofMillis(1000), 1, null))
                .getFirst();
        sleep(300);

        store.renew(List.of(new LeaseRef(1, t.revision())), LEASE);
        sleep(800);

        assertThat(store.failExpired(1, t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.LOST);
        assertThat(store.reclaim(10)).isEmpty();
    }

    // ---- give back -----------------------------------------------------------------------------------

    @Test
    void rescheduleGivesTheAttemptBackAndMovesToScheduled() {
        var t = claimOne(1);
        var due = Instant.now().plusSeconds(90).truncatedTo(ChronoUnit.MILLIS);

        assertThat(store.reschedule(1, t.revision(), due)).isTrue();

        var back = store.task(1).orElseThrow();
        assertThat(back.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(back.attempts()).isZero();
        assertThat(back.dueAt()).isEqualTo(due);
        assertThat(back.runnerId()).isNull();
        assertThat(back.leaseUntil()).isNull();
        assertThat(back.revision()).isEqualTo(t.revision() + 1);
        assertThat(jedis.zscore(keys.scheduled(queue), "1")).isEqualTo((double) due.toEpochMilli());
        assertThat(counts().scheduled()).isEqualTo(1);
        assertThat(counts().leased()).isZero();
    }

    @Test
    void unclaimReturnsTheTaskToItsQueueAtItsOldDue() {
        var original = enqueued(1);
        var t = claim(1).getFirst();

        assertThat(store.unclaim(1, t.revision())).isTrue();

        var back = store.task(1).orElseThrow();
        assertThat(back.state()).isEqualTo(TaskState.QUEUED);
        assertThat(back.attempts()).isZero();
        assertThat(back.dueAt()).isEqualTo(original.dueAt());
        assertThat(back.runnerId()).isNull();
        assertThat(back.leaseUntil()).isNull();
        assertThat(back.revision()).isEqualTo(t.revision() + 1);
        assertThat(jedis.zscore(keys.queue(queue, type), "1"))
                .isEqualTo((double) original.dueAt().toEpochMilli());
        assertThat(claim(1)).hasSize(1);
    }

    private void assertNoTransitionApplies(long id, int revision) {
        assertThat(store.ack(id, revision, true, true, null)).isFalse();
        assertThat(store.fail(id, revision, failure(1), null, true, null)).isEqualTo(FailOutcome.LOST);
        assertThat(store.reschedule(id, revision, Instant.now())).isFalse();
        assertThat(store.unclaim(id, revision)).isFalse();
        assertThat(store.saveSteps(id, revision, Map.of())).isFalse();
        assertThat(store.progress(id, revision, "p")).isFalse();
        assertThat(store.renew(List.of(new LeaseRef(id, revision)), LEASE)
                        .getFirst()
                        .owned())
                .isFalse();
    }

    private void assertExpiredFailureIsLost(long id, int revision) {
        assertThat(store.failExpired(id, revision, failure(1), null, true, null))
                .isEqualTo(FailOutcome.LOST);
    }

    private void expireLease(long id) {
        var past = Instant.now().minusSeconds(5).toEpochMilli();
        jedis.hset(keys.task(id), "lease", String.valueOf(past));
        jedis.zadd(keys.leased, past, String.valueOf(id));
    }

    @Test
    void everyTransitionIsFencedByRevisionAndState() {
        var t = claimOne(1);
        var stale = t.revision() - 1;
        var wrong = t.revision() + 1;

        for (var rev : new int[] {stale, wrong}) {
            assertNoTransitionApplies(1, rev);
        }
        assertThat(store.task(1).orElseThrow().revision()).isEqualTo(t.revision());

        expireLease(1);
        for (var rev : new int[] {stale, wrong}) {
            assertExpiredFailureIsLost(1, rev);
        }
        var stillLeased = store.task(1).orElseThrow();
        assertThat(stillLeased.state()).isEqualTo(TaskState.LEASED);
        assertThat(stillLeased.revision()).isEqualTo(t.revision());

        assertThat(store.unclaim(1, t.revision())).isTrue();
        var queued = store.task(1).orElseThrow();
        assertThat(queued.state()).isEqualTo(TaskState.QUEUED);
        assertNoTransitionApplies(1, queued.revision());
        assertExpiredFailureIsLost(1, queued.revision());
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.ack(404, 0, true, true, null)).isFalse();

        var takeover = claim(1).getFirst();
        assertThat(takeover.revision()).isGreaterThan(queued.revision());
        for (var previous : new int[] {t.revision(), queued.revision()}) {
            assertNoTransitionApplies(1, previous);
        }
        expireLease(1);
        for (var previous : new int[] {t.revision(), queued.revision()}) {
            assertExpiredFailureIsLost(1, previous);
        }
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(store.failExpired(
                        1, takeover.revision(), failure(1), Instant.now().plusSeconds(5), true, null))
                .isEqualTo(FailOutcome.RETRY);
    }

    // ---- maintenance ---------------------------------------------------------------------------------

    @Test
    void promoteMovesDueTasksKeepingTheirDueAndReportsTheNext() {
        var soon = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
        var later = Instant.now().plusSeconds(7200).truncatedTo(ChronoUnit.MILLIS);
        store.enqueue(task(1).dueAt(soon).build());
        store.enqueue(task(2).dueAt(later).build());
        assertThat(store.task(1).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.task(2).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.promote(10).moved()).isZero();
        var due = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        RedisFixtures.ageDue(jedis, keys, queue, 1, due);

        var result = store.promote(10);

        assertThat(result.moved()).isEqualTo(1);
        assertThat(result.nextDue()).isEqualTo(later);
        var moved = store.task(1).orElseThrow();
        assertThat(moved.state()).isEqualTo(TaskState.QUEUED);
        assertThat(moved.dueAt()).isEqualTo(due);
        assertThat(moved.revision()).isEqualTo(1);
        assertThat(jedis.zscore(keys.queue(queue, type), "1")).isEqualTo((double) due.toEpochMilli());
        assertThat(store.promote(10).nextDue()).isEqualTo(later);
        assertThat(counts().queued()).isEqualTo(1);
        assertThat(counts().scheduled()).isEqualTo(1);
    }

    @Test
    void promoteHonoursTheLimitAndNextDueIsNullWhenNothingIsScheduled() {
        assertThat(store.nextDue()).isNull();
        var due = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        for (var i = 1; i <= 3; i++) {
            store.enqueue(task(i).dueAt(Instant.now().plusSeconds(3600)).build());
            assertThat(store.task(i).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
            RedisFixtures.ageDue(jedis, keys, queue, i, due);
        }

        assertThat(store.promote(2).moved()).isEqualTo(2);
        assertThat(store.promote(2).moved()).isEqualTo(1);
        assertThat(store.promote(2)).isEqualTo(new today.bonfire.oss.bth4j.spi.PromoteResult(0, null));
    }

    @Test
    void purgeRemovesOldTerminalTasksOnlyAndLeavesTheKeyStringOfAnOpenWindow() {
        store.enqueue(task(1).build());
        store.enqueue(task(2).build());
        store.enqueue(task(3).uniqueKey("k").keyKeep(Duration.ofMinutes(5)).build());
        store.enqueue(task(4).build());
        var claimed = claim(4);
        store.ack(1, claimed.get(0).revision(), true, true, null);
        store.ack(2, claimed.get(1).revision(), true, true, null);
        store.ack(3, claimed.get(2).revision(), true, true, null);
        store.fail(4, claimed.get(3).revision(), failure(1), null, true, null);
        sleep(150);

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMinutes(1), 10)).isZero();
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10)).isEqualTo(3);

        assertThat(store.task(1)).isEmpty();
        assertThat(store.task(3)).isEmpty();
        assertThat(store.task(4)).isPresent();
        assertThat(jedis.get(keys.uniqueKey("k"))).isEqualTo("3");
        assertThat(jedis.pttl(keys.uniqueKey("k"))).isPositive();
        assertThat(counts().succeeded()).isZero();
        assertThat(counts().dead()).isEqualTo(1);
        assertThat(store.purge(TaskState.DEAD, Duration.ofMillis(100), 10)).isEqualTo(1);
        assertThatThrownBy(() -> store.purge(TaskState.QUEUED, Duration.ZERO, 1))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void purgeRemovesEveryKeyOfTheTasksItRemoves() {
        var done = leasedWithEveryKey(1);
        store.ack(1, done.revision(), true, true, null);
        var dead = leasedWithEveryKey(2);
        store.fail(2, dead.revision(), failure(2), null, true, null);
        var held = RedisFixtures.leasedWithHistory(
                store,
                queue,
                claimEnqueued(task(3).encodedPayload("p").uniqueKey("held").keyKeep(Duration.ofMinutes(5))));
        store.ack(3, held.revision(), true, true, null);
        RedisFixtures.assertPopulated(jedis, keys, 1);
        RedisFixtures.assertPopulated(jedis, keys, 2);

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isEqualTo(2);
        assertThat(store.purge(TaskState.DEAD, Duration.ZERO, 10)).isEqualTo(1);

        RedisFixtures.assertGone(jedis, keys, 1);
        RedisFixtures.assertGone(jedis, keys, 2);
        RedisFixtures.assertGone(jedis, keys, 3);
        assertThat(jedis.get(keys.uniqueKey("held"))).isEqualTo("3");
        assertThat(counts().succeeded()).isZero();
    }

    private TaskRecord claimEnqueued(TaskRecord.Builder b) {
        assertThat(store.enqueue(b.build())).isEqualTo(EnqueueOutcome.STORED);
        return claim(1).getFirst();
    }

    @Test
    void purgeHonoursTheLimitAndTakesHeldTasksToo() {
        store.enqueue(task(1).uniqueKey("k").keyKeep(Duration.ofMinutes(5)).build());
        for (var i = 2; i <= 5; i++) {
            store.enqueue(task(i).build());
        }
        for (var t : claim(5)) {
            store.ack(t.id(), t.revision(), true, true, null);
        }
        sleep(50);

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(2);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isEqualTo(3);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isZero();
        assertThat(store.task(1)).isEmpty();
        assertThat(store.enqueue(task(6).uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(1));
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Test
    void countsCoverEveryQueueAndState() {
        var other = store.queueId("other");
        store.enqueue(task(1).build());
        store.enqueue(task(2).dueAt(Instant.now().plusSeconds(60)).build());
        store.enqueue(task(3).queueId(other).build());

        var counts = store.counts();

        assertThat(counts.get(queue)).isEqualTo(new StateCounts(1, 1, 0, 0, 0, 0, 0));
        assertThat(counts.get(other).queued()).isEqualTo(1);
        assertThat(counts.get(queue).of(TaskState.SCHEDULED)).isEqualTo(1);
    }

    @Test
    void terminalTasksListInKeysetPagesAcrossQueues() {
        var other = store.queueId("other");
        var done = new ArrayList<Long>();
        for (var i = 1; i <= 12; i++) {
            store.enqueue(task(i).queueId(i % 3 == 0 ? other : queue).build());
        }
        for (var t : store.claim(new ClaimRequest(
                List.of(new QueueGrant(queue, 12, 12), new QueueGrant(other, 12, 12)), 12, LEASE, 1, null))) {
            store.ack(t.id(), t.revision(), true, true, null);
        }

        String cursor = null;
        do {
            var page = store.tasks(TaskState.SUCCEEDED, null, cursor, 5);
            assertThat(page.items().size()).isLessThanOrEqualTo(5);
            page.items().forEach(t -> done.add(t.id()));
            cursor = page.next();
        } while (cursor != null);

        assertThat(done).hasSize(12).doesNotHaveDuplicates();
        var byTime = new ArrayList<TaskRecord>();
        done.forEach(id -> byTime.add(store.task(id).orElseThrow()));
        assertThat(byTime)
                .isSortedAccordingTo(java.util.Comparator.comparing(TaskRecord::stateChangedAt)
                        .thenComparing(t -> Long.toString(t.id())));
        assertThat(store.tasks(TaskState.SUCCEEDED, other, null, 100).items())
                .extracting(TaskRecord::id)
                .containsExactlyInAnyOrder(3L, 6L, 9L, 12L);
        assertThat(store.tasks(TaskState.DEAD, null, null, 10).items()).isEmpty();
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
        String cursor = null;
        do {
            var page = store.tasks(TaskState.QUEUED, queue, cursor, 400);
            seen.addAll(page.items());
            cursor = page.next();
        } while (cursor != null);

        assertThat(seen).hasSize(1100);
        assertThat(seen.stream().map(TaskRecord::id).distinct().count()).isEqualTo(1100);
        assertThat(seen)
                .isSortedAccordingTo(
                        java.util.Comparator.comparing(TaskRecord::dueAt).thenComparing(t -> Long.toString(t.id())));
    }

    /** Gives the queued tasks one due time, as a batch stored in one millisecond has. */
    private void dueTogether(long... ids) {
        for (var id : ids) {
            var typeId = Integer.parseInt(jedis.hget(keys.task(id), "ty"));
            jedis.zadd(keys.queue(queue, typeId), 5_000, String.valueOf(id));
            jedis.hset(keys.task(id), "due", "5000");
        }
    }

    @Test
    void tasksDueTogetherAreListedInTheOrderAClaimTakesThemTheIdAsText() {
        for (var id : new long[] {9, 10, 100, 11}) {
            enqueued(id);
        }
        dueTogether(9, 10, 100, 11);

        var first = store.tasks(TaskState.QUEUED, queue, null, 2);
        var second = store.tasks(TaskState.QUEUED, queue, first.next(), 10).items();

        assertThat(first.items()).extracting(TaskRecord::id).containsExactly(10L, 100L);
        assertThat(second).extracting(TaskRecord::id).containsExactly(11L, 9L);
        assertThat(claim(4)).extracting(TaskRecord::id).containsExactly(10L, 100L, 11L, 9L);
    }

    @Test
    void aPageFindsItsPlaceAmongTasksDueTogetherWhenTheCursorTaskIsOfAnotherTypeOrGone() {
        var otherType = store.typeId("report");
        var ids = new ArrayList<Long>();
        for (var id = 1L; id <= 60; id++) {
            store.enqueue(task(id).typeId(id % 2 == 0 ? type : otherType).build());
            ids.add(id);
        }
        dueTogether(ids.stream().mapToLong(Long::longValue).toArray());

        var seen = new ArrayList<Long>();
        String cursor = null;
        do {
            var page = store.tasks(TaskState.QUEUED, queue, cursor, 7);
            page.items().forEach(t -> seen.add(t.id()));
            cursor = page.next();
            if (!page.items().isEmpty() && cursor != null) {
                var last = page.items().getLast();
                assertThat(store.delete(last.id(), last.revision(), null)).isEqualTo(DeleteOutcome.DELETED);
            }
        } while (cursor != null);

        assertThat(seen)
                .containsExactlyElementsOf(ids.stream()
                        .sorted(java.util.Comparator.comparing(String::valueOf))
                        .toList());
    }

    @Test
    void queuedScheduledAndLeasedListingsFilterByState() {
        var other = store.queueId("other");
        store.enqueue(task(1).build());
        store.enqueue(task(2).dueAt(Instant.now().plusSeconds(60)).build());
        store.enqueue(
                task(3).queueId(other).dueAt(Instant.now().plusSeconds(90)).build());
        store.enqueue(task(4).build());
        claim(1);

        assertThat(store.tasks(TaskState.QUEUED, null, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(4L);
        assertThat(store.tasks(TaskState.LEASED, queue, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(1L);
        assertThat(store.tasks(TaskState.SCHEDULED, null, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(2L, 3L);
        assertThat(store.tasks(TaskState.SCHEDULED, other, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(3L);
        var afterTask2 = store.tasks(TaskState.SCHEDULED, null, null, 1).next();
        assertThat(store.tasks(TaskState.SCHEDULED, null, afterTask2, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(3L);
    }

    // ---- schedules -----------------------------------------------------------------------------------

    private StoredSchedule definition(String id, String cron) {
        return new StoredSchedule(id, type, 2, queue, "acme", cron, "{\"d\":1}", 5, 0);
    }

    private StoredSchedule otherData(String id, String cron) {
        return new StoredSchedule(id, type, 2, queue, "acme", cron, "{\"d\":2}", 5, 0);
    }

    @Test
    void recurringDefinitionsAreStoredListedAndDeleted() {
        var fire = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);

        store.putSchedule(definition("s", "* * * * *"), fire, fire);

        assertThat(store.nextFireTimes()).containsEntry("s", fire);
        assertThat(store.dueSchedules(fire.minusSeconds(1), 10)).isEmpty();
        var due = store.dueSchedules(fire, 10).getFirst();
        assertThat(due.scheduleId()).isEqualTo("s");
        assertThat(due.fireAt()).isEqualTo(fire);
        var d = due.definition();
        assertThat(d.typeId()).isEqualTo(type);
        assertThat(d.queueId()).isEqualTo(queue);
        assertThat(d.payloadVersion()).isEqualTo(2);
        assertThat(d.account()).isEqualTo("acme");
        assertThat(d.cron()).isEqualTo("* * * * *");
        assertThat(d.encodedPayload()).isEqualTo("{\"d\":1}");
        assertThat(d.maxAttempts()).isEqualTo(5);
        assertThat(d.revision()).isPositive();
        assertThat(store.deleteSchedule("s")).isTrue();
        assertThat(store.deleteSchedule("s")).isFalse();
        assertThat(store.nextFireTimes()).isEmpty();
        assertThat(jedis.exists(keys.cronDefinition("s"))).isFalse();
    }

    @Test
    void replacingADefinitionKeepsTheFireTimeOnlyWhenTheCronIsUnchanged() {
        var first = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        var second = first.plusSeconds(500);
        store.putSchedule(definition("s", "* * * * *"), first, first);
        var rev1 = store.dueSchedules(first, 1).getFirst().definition().revision();

        assertThat(store.putSchedule(definition("s", "* * * * *"), second, second))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.UNCHANGED);
        assertThat(store.nextFireTimes()).containsEntry("s", first);
        assertThat(store.dueSchedules(first, 1).getFirst().definition().revision())
                .isEqualTo(rev1);
        assertThat(store.putSchedule(otherData("s", "* * * * *"), second, second))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CHANGED);
        assertThat(store.nextFireTimes()).containsEntry("s", first);
        var rev2 = store.dueSchedules(first, 1).getFirst().definition().revision();
        assertThat(store.putSchedule(definition("s", "0 * * * *"), second, second))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CHANGED);

        assertThat(rev2).isGreaterThan(rev1);
        assertThat(store.nextFireTimes()).containsEntry("s", second);
    }

    @Test
    void commitStoresTheOccurrenceScheduledAndAdvancesTheDefinitionOnce() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        var next = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        store.putSchedule(definition("s", "* * * * *"), fire, fire);
        var due = store.dueSchedules(Instant.now(), 10).getFirst();
        var occurrence = task(50).scheduleId("s").dueAt(fire).build();

        assertThat(store.commitSchedule(due, next, occurrence).isCommitted()).isTrue();

        var stored = store.task(50).orElseThrow();
        assertThat(stored.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(stored.dueAt()).isEqualTo(fire);
        assertThat(stored.scheduleId()).isEqualTo("s");
        assertThat(store.nextFireTimes()).containsEntry("s", next);
        assertThat(jedis.zscore(keys.scheduled(queue), "50")).isEqualTo((double) fire.toEpochMilli());
        assertThat(store.commitSchedule(due, next.plusSeconds(60), task(51).build())
                        .isCommitted())
                .isFalse();
        assertThat(store.task(51)).isEmpty();
        assertThat(store.promote(10).moved()).isEqualTo(1);
    }

    @Test
    void commitIsRejectedForAReplacedOrDeletedDefinition() {
        var fire = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        store.putSchedule(definition("s", "* * * * *"), fire, fire);
        var due = store.dueSchedules(Instant.now(), 10).getFirst();
        store.putSchedule(otherData("s", "* * * * *"), fire, fire);

        assertThat(store.commitSchedule(due, fire.plusSeconds(60), task(60).build())
                        .isCommitted())
                .isFalse();

        var fresh = store.dueSchedules(Instant.now(), 10).getFirst();
        store.deleteSchedule("s");
        assertThat(store.commitSchedule(fresh, fire.plusSeconds(60), task(61).build())
                        .isCommitted())
                .isFalse();
        assertThat(store.task(60)).isEmpty();
        assertThat(store.task(61)).isEmpty();
    }

    @Test
    void storeFailuresSurfaceAsTaskStoreException() {
        try (var dead = RedisClient.builder()
                .hostAndPort(new HostAndPort("localhost", 1))
                .build()) {
            var broken = RedisTaskStore.builder().client(dead).namespace("x").build();

            assertThatThrownBy(() -> broken.allocateIds(1)).isInstanceOf(TaskStoreException.class);
        }
    }

    @Test
    void namespaceIsValidated() {
        assertThatThrownBy(() ->
                        RedisTaskStore.builder().client(null).namespace("ok").build())
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() ->
                        RedisTaskStore.builder().client(jedis).namespace(" ").build())
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() ->
                        RedisTaskStore.builder().client(jedis).namespace("a{b").build())
                .isInstanceOf(TaskConfigurationException.class);
    }

    private static void awaitExpiry(BooleanSupplier expired) {
        var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!expired.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Redis did not expire the keys within 10 seconds");
            }
            sleep(20);
        }
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
