package today.bonfire.oss.bth4j.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * Two store instances on one database file, each with its own connection pool, as two processes on one machine
 * would be. The tests check what a single instance cannot show: work is shared and never claimed twice, and what
 * one instance writes the other reads.
 */
@Timeout(120)
class SqliteSharedFileTest {
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final int CLAIMERS_PER_STORE = 4;
    private static final AtomicInteger NAMESPACES = new AtomicInteger();

    private static SqliteTestSupport database;

    private final MutableClock clock = new MutableClock(Instant.parse("2100-06-15T12:00:00Z"));
    private HikariDataSource otherPool;
    private SqliteTaskStore first;
    private SqliteTaskStore second;
    private int queue;
    private int type;

    @BeforeAll
    static void openDatabase() {
        database = SqliteTestSupport.open();
    }

    @AfterAll
    static void closeDatabase() {
        database.close();
    }

    @BeforeEach
    void setUp() {
        var prefix = SqliteTestSupport.prefix("shared" + NAMESPACES.incrementAndGet());
        first = SqliteTaskStore.builder()
                .dataSource(database.dataSource())
                .namespace(prefix)
                .clock(clock)
                .build();
        first.init();
        otherPool = database.anotherPool();
        second = SqliteTaskStore.builder()
                .dataSource(otherPool)
                .namespace(prefix)
                .clock(clock)
                .build();
        second.init();
        queue = first.queueId("default");
        type = first.typeId("send-mail");
    }

    @AfterEach
    void tearDown() {
        otherPool.close();
    }

    private List<TaskRecord> enqueueThrough(SqliteTaskStore store, int count) {
        var first = store.allocateIds(count);
        var tasks = IntStream.range(0, count)
                .mapToObj(i -> SqliteTestSupport.task(first + i, queue, type, clock.instant())
                        .build())
                .toList();
        assertThat(store.enqueueAll(tasks)).containsOnly(EnqueueOutcome.STORED);
        return tasks;
    }

    private List<Long> claimUntilEmpty(SqliteTaskStore store, int runnerId) {
        var claimed = new ArrayList<Long>();
        while (true) {
            var batch = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, runnerId, null));
            if (batch.isEmpty()) return claimed;
            batch.forEach(t -> claimed.add(t.id()));
        }
    }

    @Test
    void twoInstancesShareTheQueueAndNeverClaimATaskTwice() throws Exception {
        int total = 400;
        var ids = enqueueThrough(first, total).stream().map(TaskRecord::id).toList();
        var claimedBy = new ConcurrentLinkedQueue<Long>();
        var executor = Executors.newFixedThreadPool(2 * CLAIMERS_PER_STORE);
        try {
            var futures = new ArrayList<Future<?>>();
            for (var store : List.of(first, second)) {
                for (int i = 0; i < CLAIMERS_PER_STORE; i++) {
                    int runnerId = store.allocateRunnerId("claimer-" + futures.size(), Set.of(queue), Set.of(type));
                    futures.add(executor.submit(() -> claimedBy.addAll(claimUntilEmpty(store, runnerId))));
                }
            }
            for (var future : futures) future.get(60, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(claimedBy).hasSize(total).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(ids);
        assertThat(first.counts().get(queue).leased()).isEqualTo(total);
        assertThat(second.counts().get(queue).queued()).isZero();
    }

    @Test
    void concurrentEnqueuesFromBothInstancesAllArrive() throws Exception {
        int perWriter = 50;
        var executor = Executors.newFixedThreadPool(4);
        try {
            var futures = new ArrayList<Future<List<TaskRecord>>>();
            for (var store : List.of(first, second)) {
                for (int i = 0; i < 2; i++) {
                    futures.add(executor.submit(() -> {
                        var stored = new ArrayList<TaskRecord>();
                        for (int n = 0; n < perWriter; n++) stored.addAll(enqueueThrough(store, 1));
                        return stored;
                    }));
                }
            }
            var ids = new HashSet<Long>();
            for (var future : futures) {
                for (var task : future.get(60, TimeUnit.SECONDS))
                    assertThat(ids.add(task.id())).isTrue();
            }
            assertThat(ids).hasSize(4 * perWriter);
            assertThat(first.counts().get(queue).queued()).isEqualTo(4L * perWriter);
            assertThat(second.counts().get(queue).queued()).isEqualTo(4L * perWriter);
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError(e);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void whatOneInstanceWritesTheOtherReadsAndFences() {
        int runnerA = first.allocateRunnerId("a", Set.of(queue), Set.of(type));
        int runnerB = second.allocateRunnerId("b", Set.of(queue), Set.of(type));
        var task = enqueueThrough(first, 1).getFirst();

        assertThat(runnerB).isNotEqualTo(runnerA);
        assertThat(second.queueId("default")).isEqualTo(queue);
        assertThat(second.typeId("send-mail")).isEqualTo(type);
        assertThat(second.task(task.id())).isPresent();
        var claimed = first.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, runnerA, null))
                .getFirst();
        assertThat(second.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, runnerB, null)))
                .isEmpty();
        assertThat(second.task(task.id()).orElseThrow().state()).isEqualTo(TaskState.LEASED);

        assertThat(second.ack(claimed.id(), claimed.revision() + 1, true, true, null))
                .isFalse();
        assertThat(second.ack(claimed.id(), claimed.revision(), true, true, null))
                .isTrue();
        assertThat(first.ack(claimed.id(), claimed.revision(), true, true, null))
                .isFalse();
        assertThat(first.task(task.id()).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(first.runners()).extracting(w -> w.id()).containsExactlyInAnyOrder(runnerA, runnerB);
    }

    @Test
    void anExpiredLeaseOfOneInstanceIsEndedByTheOther() {
        int runnerA = first.allocateRunnerId("a", Set.of(queue), Set.of(type));
        var task = enqueueThrough(first, 1).getFirst();
        var claimed = first.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, LEASE, runnerA, null))
                .getFirst();
        assertThat(second.reclaim(10)).isEmpty();

        clock.advance(LEASE.plusSeconds(1));
        var expired = second.reclaim(10);

        assertThat(expired).singleElement().satisfies(l -> assertThat(l.id()).isEqualTo(task.id()));
        var failure = new FailureInfo(1, clock.instant(), "lease expired", "lease expired", null);
        assertThat(second.failExpired(claimed.id(), claimed.revision(), failure, clock.instant(), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(first.ack(claimed.id(), claimed.revision(), true, true, null))
                .isFalse();
        assertThat(first.task(task.id()).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
    }
}
