package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

class DefaultAdminOpsCoverageTest {
    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final int queue = store.queueId("main");
    private final int otherQueue = store.queueId("other");
    private final int type = store.typeId("mail");
    private final int otherType = store.typeId("export");
    private final List<List<Object>> samples = new ArrayList<>();
    private final FullStore observed = (FullStore) Proxy.newProxyInstance(
            FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("tasks")) samples.add(java.util.Arrays.asList(args));
                try {
                    return method.invoke(store, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    private final DefaultAdminOps admin = new DefaultAdminOps(
            observed,
            new Names(observed),
            new QueuesHolder(Set.of("main", "other"), List.of("main"), "main"),
            clock,
            null,
            null);

    private void enqueue(int queueId, int typeId) {
        assertThat(store.enqueue(TaskRecord.builder()
                        .id(store.allocateIds(1))
                        .queueId(queueId)
                        .typeId(typeId)
                        .createdAt(clock.instant())
                        .build()))
                .isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void reportsWholeQueueCountWithoutSamplingAndIgnoresScheduledWork() {
        enqueue(queue, type);
        enqueue(queue, otherType);
        store.enqueue(TaskRecord.builder()
                .id(store.allocateIds(1))
                .queueId(otherQueue)
                .typeId(type)
                .due(clock.instant().plusSeconds(1))
                .build());

        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", null, 2));
        assertThat(samples).isEmpty();
    }

    @Test
    void staleWorkerIsIgnoredAndHeartbeatRestoresCoverage() {
        enqueue(queue, type);
        int worker = store.allocateWorkerId("worker", Set.of(queue), Set.of(type));
        clock.advance(Duration.ofSeconds(61));

        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", null, 1));
        store.heartbeat(worker, "worker", Set.of(queue), Set.of(type), 0, true);
        assertThat(admin.unservedWork()).isEmpty();
    }

    @Test
    void heartbeatAtTheCutoffIsLiveAndCustomWindowUsesAdminClock() {
        enqueue(queue, type);
        store.allocateWorkerId("worker", Set.of(queue), Set.of(type));
        clock.advance(Duration.ofMinutes(1));

        assertThat(admin.unservedWork()).isEmpty();
        clock.advance(Duration.ofNanos(1));
        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", null, 1));
        assertThat(admin.unservedWork(Duration.ofMinutes(2))).isEmpty();
    }

    @Test
    void pausedQueueIsSkippedWithoutSampling() {
        enqueue(queue, type);
        store.pauseQueue(queue, true);

        assertThat(admin.unservedWork()).isEmpty();
        assertThat(samples).isEmpty();
        store.pauseQueue(queue, false);
        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", null, 1));
    }

    @Test
    void reportsTypesOnlyAgainstWorkersOfThatQueue() {
        enqueue(queue, otherType);
        enqueue(queue, otherType);
        enqueue(queue, type);
        store.allocateWorkerId("main", Set.of(queue), Set.of(type));
        store.allocateWorkerId("other", Set.of(otherQueue), Set.of(otherType));

        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", "export", 2));
        assertThat(samples).isEmpty();
    }

    @Test
    void reportsExactCountsIncludingTypesBeyondTheFirst100Tasks() {
        store.allocateWorkerId("worker", Set.of(queue, otherQueue), Set.of());
        for (int i = 0; i < 105; i++) enqueue(queue, type);
        enqueue(queue, otherType);
        enqueue(otherQueue, otherType);

        assertThat(admin.unservedWork())
                .containsExactlyInAnyOrder(
                        new UnservedWork("main", "mail", 105),
                        new UnservedWork("main", "export", 1),
                        new UnservedWork("other", "export", 1));
        assertThat(samples).isEmpty();
    }

    @Test
    void combinesCoverageOfLiveWorkersAndReportsNothingWhenServed() {
        enqueue(queue, type);
        enqueue(queue, otherType);
        store.allocateWorkerId("mail", Set.of(queue), Set.of(type));
        store.allocateWorkerId("export", Set.of(queue), Set.of(otherType));

        assertThat(admin.unservedWork()).isEmpty();
    }

    @Test
    void aWorkerWithEmptyCoverageServesNoQueue() {
        enqueue(queue, type);
        store.allocateWorkerId("empty", Set.of(), Set.of());

        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", null, 1));
    }

    @Test
    void quietWorkersDoNotServeQueuesOrContributeTypeCoverage() {
        enqueue(queue, type);
        enqueue(queue, otherType);
        int quiet = store.allocateWorkerId("quiet", Set.of(queue), Set.of(type, otherType));
        store.heartbeat(quiet, "quiet", Set.of(queue), Set.of(type, otherType), 1, false);
        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", null, 2));

        store.allocateWorkerId("accepting", Set.of(queue), Set.of(type));
        assertThat(admin.unservedWork()).containsExactly(new UnservedWork("main", "export", 1));
        store.heartbeat(quiet, "quiet", Set.of(queue), Set.of(type, otherType), 0, true);
        assertThat(admin.unservedWork()).isEmpty();
    }

    @Test
    void negativeWindowIsRejected() {
        assertThatThrownBy(() -> admin.unservedWork(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
