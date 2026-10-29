package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
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
import today.bonfire.oss.bth4j.JsonMapperTest;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.Keys;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.service.TaskOps.FailOutcome;

/**
 * Every state transition of {@link TaskOps} against a real Redis. Needs Docker; run by failsafe.
 * Each test uses its own namespace, so tests do not see each other's keys.
 */
@Testcontainers
class TaskOpsIT {

    public static class Payload {
        private String name;
        private int count;

        public Payload() {}

        Payload(String name, int count) {
            this.name = name;
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration DEAD_RETENTION = Duration.ofDays(1);

    private static RedisClient jedis;

    private Keys keys;
    private QueuesHolder queues;
    private TaskOps ops;

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

    @BeforeEach
    void setUp() {
        keys = new Keys("T" + COUNTER.incrementAndGet());
        queues = new QueuesHolder(Set.of("def", "hi"), List.of("def", "hi"), "def");
        ops = new TaskOps(keys, jedis, new JsonMapperTest(), TestEvents.UNKNOWN::from, queues, DEAD_RETENTION);
    }

    private static Task.Builder task(String id, TestEvents event) {
        return Task.Builder.newTask(id).event(event).accountId(7);
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws InterruptedException {
        var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.sleep(10);
        }
    }

    private List<Task> claim(List<String> queueNames, int count) {
        return ops.claim(queueNames, count, "tok" + COUNTER.incrementAndGet(), LEASE);
    }

    private Task claimOne(String id, TestEvents event) {
        assertThat(ops.enqueue(task(id, event).build())).isTrue();
        var claimed = claim(List.of("def"), 1);
        assertThat(claimed).hasSize(1);
        return claimed.getFirst();
    }

    private Task makeDead(String id) {
        var t = claimOne(id, TestEvents.REGULAR_TASK);
        assertThat(ops.fail(t, Duration.ZERO, "boom")).isEqualTo(FailOutcome.DEAD);
        return t;
    }

    // ---- enqueue ----

    @Test
    void enqueueStoresAndQueues() {
        assertThat(ops.enqueue(task("a", TestEvents.REGULAR_TASK).build())).isTrue();

        assertThat(ops.queueSize("def")).isEqualTo(1);
        assertThat(ops.scheduledCount()).isZero();
        var stored = ops.getTask("a").orElseThrow();
        assertThat(stored.event()).isEqualTo(TestEvents.REGULAR_TASK);
        assertThat(stored.accountId()).isEqualTo("7");
        assertThat(stored.queueName()).isEqualTo("def");
        assertThat(stored.attempts()).isZero();
        assertThat(stored.maxAttempts()).isEqualTo(1);
    }

    @Test
    void enqueueWithSameIdIsRejectedAndLeavesOneEntry() {
        assertThat(ops.enqueue(task("a", TestEvents.REGULAR_TASK).build())).isTrue();
        assertThat(ops.enqueue(task("a", TestEvents.REGULAR_TASK).build())).isFalse();

        assertThat(ops.queueSize("def")).isEqualTo(1);
    }

    @Test
    void enqueueWithExecutionTimeGoesToScheduledSet() {
        var due = Instant.now().plus(Duration.ofHours(1));

        assertThat(ops.enqueue(task("a", TestEvents.REGULAR_TASK)
                        .executeAtTimeStamp(due)
                        .build()))
                .isTrue();

        assertThat(ops.scheduledCount()).isEqualTo(1);
        assertThat(ops.queueSize("def")).isZero();
        assertThat(ops.getTask("a").orElseThrow().executeAtTimeStamp())
                .isEqualTo(Instant.ofEpochMilli(due.toEpochMilli()));
    }

    @Test
    void enqueueToUnknownQueueThrows() {
        var t = task("a", TestEvents.REGULAR_TASK).queueName("nope").build();

        assertThatThrownBy(() -> ops.enqueue(t)).isInstanceOf(TaskErrorException.class);
        assertThat(ops.getTask("a")).isEmpty();
    }

    @Test
    void enqueueToNamedQueue() {
        assertThat(ops.enqueue(
                        task("a", TestEvents.REGULAR_TASK).queueName("hi").build()))
                .isTrue();

        assertThat(ops.queueSize("hi")).isEqualTo(1);
        assertThat(ops.queueSize("def")).isZero();
    }

    @Test
    void enqueueWithDataStoresJson() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build(), new Payload("n", 3));

        var typed = ops.getDataForTask("a", Payload.class);
        assertThat(typed.getName()).isEqualTo("n");
        assertThat(typed.getCount()).isEqualTo(3);
        assertThat(ops.getDataForTask("a")).contains("\"name\":\"n\"");
        assertThat(ops.getTask("a").orElseThrow().data()).isEqualTo(ops.getDataForTask("a"));
    }

    @Test
    void dataOfTaskWithoutDataOrMissingTaskIsNull() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());

        assertThat(ops.getDataForTask("a", Payload.class)).isNull();
        assertThat(ops.getDataForTask("missing", Payload.class)).isNull();
        assertThat(ops.getTask("missing")).isEmpty();
    }

    // ---- claim ----

    @Test
    void claimReturnsAtMostCountInFifoOrder() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());
        ops.enqueue(task("b", TestEvents.REGULAR_TASK).build());
        ops.enqueue(task("c", TestEvents.REGULAR_TASK).build());

        var first = claim(List.of("def"), 2);

        assertThat(first).extracting(Task::uniqueId).containsExactly("a", "b");
        assertThat(ops.queueSize("def")).isEqualTo(1);
        var second = claim(List.of("def"), 5);
        assertThat(second).extracting(Task::uniqueId).containsExactly("c");
    }

    @Test
    void claimTriesQueuesInGivenOrder() {
        ops.enqueue(task("d", TestEvents.REGULAR_TASK).build());
        ops.enqueue(task("h", TestEvents.REGULAR_TASK).queueName("hi").build());

        var claimed = claim(List.of("hi", "def"), 5);

        assertThat(claimed).extracting(Task::uniqueId).containsExactly("h", "d");
    }

    @Test
    void claimLeasesTheTaskAndCarriesData() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build(), new Payload("n", 1));

        var claimed = claim(List.of("def"), 1).getFirst();

        assertThat(claimed.attempts()).isEqualTo(1);
        assertThat(claimed.token()).isNotBlank();
        assertThat(claimed.data()).contains("\"name\":\"n\"");
        assertThat(ops.decodeData(claimed, Payload.class).getCount()).isEqualTo(1);
        assertThat(ops.leasedCount()).isEqualTo(1);
        assertThat(jedis.zrange(keys.leased, 0, -1)).containsExactly("a");
        assertThat(ops.queueSize("def")).isZero();
    }

    @Test
    void claimOnEmptyQueuesReturnsEmptyList() {
        assertThat(claim(List.of("def", "hi"), 5)).isEmpty();
        assertThat(ops.leasedCount()).isZero();
    }

    @Test
    void everyClaimedTaskGetsItsOwnToken() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());
        ops.enqueue(task("b", TestEvents.REGULAR_TASK).build());

        var claimed = claim(List.of("def"), 2);

        assertThat(claimed).extracting(Task::token).doesNotHaveDuplicates();
    }

    // ---- renew ----

    @Test
    void renewCountsOwnedLeasesAndIsZeroAfterAck() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());
        ops.enqueue(task("b", TestEvents.REGULAR_TASK).build());
        var claimed = claim(List.of("def"), 2);

        assertThat(ops.renew(claimed, LEASE)).isEqualTo(2);
        assertThat(ops.ack(claimed.get(0))).isTrue();
        assertThat(ops.renew(claimed, LEASE)).isEqualTo(1);
        assertThat(ops.ack(claimed.get(1))).isTrue();
        assertThat(ops.renew(claimed, LEASE)).isZero();
        assertThat(ops.renew(List.of(), LEASE)).isZero();
    }

    // ---- ack ----

    @Test
    void ackSucceedsOnceAndRemovesTheTask() {
        var t = claimOne("a", TestEvents.REGULAR_TASK);

        assertThat(ops.ack(t)).isTrue();
        assertThat(ops.ack(t)).isFalse();

        assertThat(ops.getTask("a")).isEmpty();
        assertThat(ops.leasedCount()).isZero();
    }

    // ---- fail ----

    @Test
    void failWithAttemptsLeftSchedulesRetry() {
        var t = claimOne("a", TestEvents.RETRY_TASK);

        var before = System.currentTimeMillis();
        var outcome = ops.fail(t, Duration.ofMillis(100), "oops");
        var after = System.currentTimeMillis();

        assertThat(outcome).isEqualTo(FailOutcome.RETRY);
        assertThat(ops.leasedCount()).isZero();
        assertThat(ops.scheduledCount()).isEqualTo(1);
        var stored = ops.getTask("a").orElseThrow();
        assertThat(stored.executeAtTimeStamp().toEpochMilli()).isBetween(before + 100, after + 100);
        assertThat(stored.lastError()).isEqualTo("oops");
        assertThat(stored.attempts()).isEqualTo(1);
    }

    @Test
    void failWithStaleTokenIsLost() {
        var t = claimOne("a", TestEvents.RETRY_TASK);
        assertThat(ops.fail(t, Duration.ofMillis(100), "oops")).isEqualTo(FailOutcome.RETRY);

        assertThat(ops.fail(t, Duration.ofMillis(100), "again")).isEqualTo(FailOutcome.LOST);

        assertThat(ops.scheduledCount()).isEqualTo(1);
        assertThat(ops.getTask("a").orElseThrow().lastError()).isEqualTo("oops");
    }

    @Test
    void failWithoutAttemptsLeftKillsTheTask() {
        var t = claimOne("a", TestEvents.REGULAR_TASK);

        assertThat(ops.fail(t, Duration.ofMillis(100), "boom")).isEqualTo(FailOutcome.DEAD);

        assertThat(jedis.hget(keys.task("a"), "error")).isEqualTo("boom");
        assertThat(ops.deadCount()).isEqualTo(1);
        assertThat(ops.scheduledCount()).isZero();
        assertThat(ops.leasedCount()).isZero();
        var dead = ops.deadTasks(10);
        assertThat(dead).hasSize(1);
        assertThat(dead.getFirst().uniqueId()).isEqualTo("a");
        assertThat(dead.getFirst().lastError()).isEqualTo("boom");
        assertThat(jedis.ttl(keys.task("a"))).isPositive();
    }

    @Test
    void retriedTaskDiesAfterMaxAttempts() throws InterruptedException {
        ops.enqueue(task("a", TestEvents.RETRY_TASK).build());
        FailOutcome last = null;
        for (int i = 1; i <= 4; i++) {
            var claimed = claim(List.of("def"), 1);
            assertThat(claimed).hasSize(1);
            assertThat(claimed.getFirst().attempts()).isEqualTo(i);
            last = ops.fail(claimed.getFirst(), Duration.ZERO, "e" + i);
            if (i < 4) {
                assertThat(last).isEqualTo(FailOutcome.RETRY);
                awaitTrue("promotion", () -> ops.promote(10).moved() > 0);
            }
        }

        assertThat(last).isEqualTo(FailOutcome.DEAD);
        assertThat(ops.deadCount()).isEqualTo(1);
    }

    // ---- promote ----

    @Test
    void promoteWithNothingDueReportsEarliestDue() {
        var soon = Instant.now().plus(Duration.ofMinutes(30));
        ops.enqueue(task("late", TestEvents.REGULAR_TASK)
                .executeAfter(Duration.ofHours(1))
                .build());
        ops.enqueue(
                task("soon", TestEvents.REGULAR_TASK).executeAtTimeStamp(soon).build());

        var result = ops.promote(10);

        assertThat(result.moved()).isZero();
        assertThat(result.nextDue()).isEqualTo(Instant.ofEpochMilli(soon.toEpochMilli()));
        assertThat(ops.scheduledCount()).isEqualTo(2);
    }

    @Test
    void promoteMovesDueTasksIntoTheirQueue() throws InterruptedException {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK)
                .queueName("hi")
                .executeAfter(Duration.ofMillis(150))
                .build());
        var moved = new AtomicInteger();

        awaitTrue("task to become due", () -> {
            moved.addAndGet(ops.promote(10).moved());
            return moved.get() > 0;
        });

        assertThat(moved.get()).isEqualTo(1);
        assertThat(ops.queueSize("hi")).isEqualTo(1);
        assertThat(ops.scheduledCount()).isZero();
        assertThat(ops.promote(10).nextDue()).isNull();
        assertThat(ops.getTask("a").orElseThrow().executeAtTimeStamp()).isNull();
    }

    @Test
    void promoteRespectsTheLimit() throws InterruptedException {
        for (var id : List.of("a", "b", "c")) {
            ops.enqueue(task(id, TestEvents.REGULAR_TASK)
                    .executeAtTimeStamp(Instant.now().plusMillis(50))
                    .build());
        }
        Thread.sleep(100);

        var first = ops.promote(2);

        assertThat(first.moved()).isEqualTo(2);
        assertThat(first.nextDue()).isNotNull();
        assertThat(ops.promote(2).moved()).isEqualTo(1);
    }

    // ---- reclaim ----

    @Test
    void reclaimReturnsExpiredLeaseAndOwnerCanBeFailedByToken() throws InterruptedException {
        ops.enqueue(task("a", TestEvents.RETRY_TASK).build());
        var worker =
                ops.claim(List.of("def"), 1, "short", Duration.ofMillis(50)).getFirst();
        Thread.sleep(100);

        var expired = ops.reclaim(10);

        assertThat(expired).hasSize(1);
        var e = expired.getFirst();
        assertThat(e.id()).isEqualTo("a");
        assertThat(e.token()).isEqualTo(worker.token());
        assertThat(e.attempts()).isEqualTo(1);
        assertThat(e.maxAttempts()).isEqualTo(4);
        assertThat(e.event()).isEqualTo(TestEvents.RETRY_TASK);

        assertThat(ops.fail(e.id(), e.token(), Duration.ofMillis(10), "lease expired"))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(ops.ack(worker)).isFalse();
        assertThat(ops.getTask("a")).isPresent();
        assertThat(ops.reclaim(10)).isEmpty();
    }

    @Test
    void renewedLeaseIsNotReclaimed() throws InterruptedException {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());
        var worker =
                ops.claim(List.of("def"), 1, "short", Duration.ofMillis(50)).getFirst();

        assertThat(ops.renew(List.of(worker), LEASE)).isEqualTo(1);
        Thread.sleep(100);

        assertThat(ops.reclaim(10)).isEmpty();
    }

    // ---- reschedule and unclaim ----

    @Test
    void rescheduleMovesToScheduledGivesAttemptBackAndRejectsOldToken() {
        var t = claimOne("a", TestEvents.REGULAR_TASK);
        var due = Instant.now().plus(Duration.ofHours(1));

        assertThat(ops.reschedule(t, due)).isTrue();

        assertThat(ops.leasedCount()).isZero();
        assertThat(ops.scheduledCount()).isEqualTo(1);
        var stored = ops.getTask("a").orElseThrow();
        assertThat(stored.attempts()).isZero();
        assertThat(stored.executeAtTimeStamp()).isEqualTo(Instant.ofEpochMilli(due.toEpochMilli()));
        assertThat(ops.ack(t)).isFalse();
        assertThat(ops.reschedule(t, due)).isFalse();
        assertThat(ops.fail(t, Duration.ZERO, "x")).isEqualTo(FailOutcome.LOST);
    }

    @Test
    void unclaimPutsTheTaskBackAtTheFront() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());
        ops.enqueue(task("b", TestEvents.REGULAR_TASK).build());
        var a = claim(List.of("def"), 1).getFirst();
        assertThat(a.uniqueId()).isEqualTo("a");

        assertThat(ops.unclaim(a)).isTrue();

        assertThat(ops.getTask("a").orElseThrow().attempts()).isZero();
        assertThat(ops.leasedCount()).isZero();
        assertThat(ops.queueSize("def")).isEqualTo(2);
        var next = claim(List.of("def"), 1).getFirst();
        assertThat(next.uniqueId()).isEqualTo("a");
        assertThat(next.attempts()).isEqualTo(1);
        assertThat(ops.unclaim(a)).isFalse();
    }

    // ---- dead tasks ----

    @Test
    void retryDeadTaskPutsItBackWithFreshAttempts() {
        makeDead("a");

        assertThat(ops.retryDeadTask("a")).isTrue();

        assertThat(ops.deadCount()).isZero();
        assertThat(ops.queueSize("def")).isEqualTo(1);
        var stored = ops.getTask("a").orElseThrow();
        assertThat(stored.attempts()).isZero();
        assertThat(stored.lastError()).isNull();
        assertThat(jedis.ttl(keys.task("a"))).isEqualTo(-1);
        assertThat(ops.retryDeadTask("a")).isFalse();
        assertThat(claim(List.of("def"), 1)).extracting(Task::uniqueId).containsExactly("a");
    }

    @Test
    void retryDeadTaskOfLiveTaskIsFalse() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());

        assertThat(ops.retryDeadTask("a")).isFalse();
        assertThat(ops.queueSize("def")).isEqualTo(1);
    }

    @Test
    void purgeDeadTasksRemovesOnlyOlderThanCutoff() {
        makeDead("a");

        assertThat(ops.purgeDeadTasks(Instant.now().minusSeconds(60))).isZero();
        assertThat(ops.deadCount()).isEqualTo(1);

        assertThat(ops.purgeDeadTasks(Instant.now().plusSeconds(1))).isEqualTo(1);
        assertThat(ops.deadCount()).isZero();
        assertThat(ops.getTask("a")).isEmpty();
    }

    // ---- recurring ----

    private static Task recurring(String id, String cron) {
        return task(id, TestEvents.RECURRING).cronExpression(cron).build();
    }

    private static void assertNextWholeMinute(Instant fire, Instant registeredBetweenStart, Instant end) {
        assertThat(fire.toEpochMilli() % 60_000).isZero();
        assertThat(fire).isAfter(registeredBetweenStart);
        assertThat(fire).isBeforeOrEqualTo(end.plusSeconds(60));
    }

    @Test
    void addRecurringTaskRegistersWithNextWholeMinute() {
        var start = Instant.now();
        assertThat(ops.addRecurringTask(recurring("r", "* * * * *"))).isEqualTo("r");
        var end = Instant.now();

        var all = ops.recurringTasks();
        assertThat(all).containsOnlyKeys("r");
        assertNextWholeMinute(all.get("r"), start, end);
    }

    @Test
    void registeringAgainWithSameCronKeepsNextFireTime() {
        ops.addRecurringTask(recurring("r", "* * * * *"));
        var first = ops.recurringTasks().get("r");

        ops.addRecurringTask(recurring("r", "* * * * *"));

        assertThat(ops.recurringTasks().get("r")).isEqualTo(first);
        assertThat(ops.recurringTasks()).hasSize(1);
    }

    @Test
    void registeringWithDifferentCronChangesNextFireTime() {
        ops.addRecurringTask(recurring("r", "* * * * *"));

        var start = Instant.now();
        ops.addRecurringTask(recurring("r", "*/2 * * * *"));
        var end = Instant.now();

        var next = ops.recurringTasks().get("r");
        assertThat(next.toEpochMilli() % 120_000).isZero();
        assertThat(next).isAfter(start);
        assertThat(next).isBeforeOrEqualTo(end.plusSeconds(120));
        var due = ops.cronDue(Instant.now().plus(Duration.ofHours(1)), 10).getFirst();
        assertThat(due.definition()).containsEntry("cron", "*/2 * * * *");
        assertThat(due.revision()).isEqualTo(2);
    }

    @Test
    void cronDueReturnsDefinitionWithRevision() {
        ops.addRecurringTask(
                task("r", TestEvents.RECURRING)
                        .cronExpression("* * * * *")
                        .queueName("hi")
                        .build(),
                new Payload("n", 5));

        assertThat(ops.cronDue(Instant.now(), 10)).isEmpty();
        var due = ops.cronDue(Instant.now().plus(Duration.ofHours(1)), 10);

        assertThat(due).hasSize(1);
        var d = due.getFirst();
        assertThat(d.scheduleId()).isEqualTo("r");
        assertThat(d.fireAt()).isEqualTo(ops.recurringTasks().get("r"));
        assertThat(d.revision()).isEqualTo(1);
        assertThat(d.definition())
                .containsEntry("event", TestEvents.RECURRING.value())
                .containsEntry("account", "7")
                .containsEntry("queue", "hi")
                .containsEntry("cron", "* * * * *");
        assertThat(d.definition().get("data")).contains("\"name\":\"n\"");
    }

    @Test
    void cronCommitCreatesOccurrenceAndAdvancesSchedule() {
        ops.addRecurringTask(recurring("r", "* * * * *"), new Payload("n", 5));
        var due = ops.cronDue(Instant.now().plus(Duration.ofHours(1)), 10).getFirst();
        var next = Task.nextExecution("* * * * *", due.fireAt()).orElseThrow();

        assertThat(ops.cronCommit(due, next)).isTrue();

        var occurrenceId = "r:" + due.fireAt().toEpochMilli();
        assertThat(jedis.zscore(keys.scheduled, occurrenceId))
                .isEqualTo((double) due.fireAt().toEpochMilli());
        var occurrence = ops.getTask(occurrenceId).orElseThrow();
        assertThat(occurrence.scheduleId()).isEqualTo("r");
        assertThat(occurrence.event()).isEqualTo(TestEvents.RECURRING);
        assertThat(occurrence.queueName()).isEqualTo("def");
        assertThat(occurrence.executeAtTimeStamp()).isEqualTo(due.fireAt());
        assertThat(ops.decodeData(occurrence, Payload.class).getCount()).isEqualTo(5);
        assertThat(ops.scheduledCount()).isEqualTo(1);
        assertThat(ops.recurringTasks().get("r")).isEqualTo(next);
    }

    @Test
    void secondCronCommitOfSameDueScheduleIsRejected() {
        ops.addRecurringTask(recurring("r", "* * * * *"));
        var due = ops.cronDue(Instant.now().plus(Duration.ofHours(1)), 10).getFirst();
        var next = Task.nextExecution("* * * * *", due.fireAt()).orElseThrow();
        assertThat(ops.cronCommit(due, next)).isTrue();

        assertThat(ops.cronCommit(due, next)).isFalse();

        assertThat(ops.scheduledCount()).isEqualTo(1);
        assertThat(ops.recurringTasks().get("r")).isEqualTo(next);
    }

    @Test
    void cronCommitAfterRedefinitionIsRejected() {
        ops.addRecurringTask(recurring("r", "* * * * *"));
        var due = ops.cronDue(Instant.now().plus(Duration.ofHours(1)), 10).getFirst();
        ops.addRecurringTask(recurring("r", "*/2 * * * *"));

        assertThat(ops.cronCommit(due, due.fireAt().plusSeconds(60))).isFalse();
        assertThat(ops.scheduledCount()).isZero();
    }

    @Test
    void deleteRecurringTaskReturnsTrueThenFalse() {
        ops.addRecurringTask(recurring("r", "* * * * *"));

        assertThat(ops.deleteRecurringTask("r")).isTrue();
        assertThat(ops.deleteRecurringTask("r")).isFalse();

        assertThat(ops.recurringTasks()).isEmpty();
        assertThat(jedis.exists(keys.cronDefinition("r"))).isFalse();
    }

    @Test
    void deleteAllRecurringTasksRemovesAll() {
        ops.addRecurringTask(recurring("r1", "* * * * *"));
        ops.addRecurringTask(recurring("r2", "*/5 * * * *"));
        ops.addRecurringTask(recurring("r3", "0 * * * *"));

        assertThat(ops.deleteAllRecurringTasks()).isEqualTo(3);

        assertThat(ops.recurringTasks()).isEmpty();
        assertThat(ops.deleteAllRecurringTasks()).isZero();
    }

    @Test
    void addRecurringTaskRequiresRecurringEventAndCron() {
        var nonRecurring = task("a", TestEvents.REGULAR_TASK).build();
        var noCron = task("b", TestEvents.RECURRING)
                .executeAfter(Duration.ofMinutes(1))
                .build();

        assertThatThrownBy(() -> ops.addRecurringTask(nonRecurring)).isInstanceOf(TaskErrorException.class);
        assertThatThrownBy(() -> ops.addRecurringTask(noCron)).isInstanceOf(TaskErrorException.class);
        assertThat(ops.recurringTasks()).isEmpty();
    }

    // ---- unknown event ----

    @Test
    void claimedTaskWithUnknownEventIsDroppedButStaysLeased() {
        ops.enqueue(task("a", TestEvents.REGULAR_TASK).build());
        var strict = new TaskOps(
                keys,
                jedis,
                new JsonMapperTest(),
                v -> TestEvents.REGULAR_TASK.value().equals(v) ? null : TestEvents.UNKNOWN.from(v),
                queues,
                DEAD_RETENTION);

        var claimed = strict.claim(List.of("def"), 5, "tok", LEASE);

        assertThat(claimed).isEmpty();
        assertThat(strict.leasedCount()).isEqualTo(1);
        assertThat(strict.queueSize("def")).isZero();
    }
}
