package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.FailureDetail;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskClient;
import today.bonfire.oss.bth4j.TaskContext;
import today.bonfire.oss.bth4j.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskType;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * The maximum run time of an attempt, driven one renewal round at a time through {@code renewOnce} against a
 * processor that blocks and ignores the cancel request, the way a call without a timeout does.
 */
@Timeout(10)
class TaskProcessorOverrunTest {
    private static final TaskType<Void> TYPE = TaskType.of("overrun-test");
    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final Duration RENEW_INTERVAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX_RUN_TIME = Duration.ofSeconds(2);

    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final Names names = new Names(store);
    private final TaskClient client =
            TaskClient.builder().store(store).clock(clock).build();
    private final int queueId = names.queueId("Q:DEF");
    private final int typeId = names.typeId(TYPE.name());
    private final Map<Long, TaskExecution> running = new ConcurrentHashMap<>();
    private final QueueReservations reservations = new QueueReservations();
    private final RunnerStatus status = new RunnerStatus(clock);
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicReference<TaskContext> context = new AtomicReference<>();
    private final CapturingExecutor executor = new CapturingExecutor();
    private Thread worker;

    private final TaskExecution.Settings settings = new TaskExecution.Settings(
            store,
            store,
            names,
            new Codecs(null),
            new TaskProcessorRegistry().register(TYPE, (ctx, payload) -> {
                context.set(ctx);
                started.countDown();
                while (true) {
                    try {
                        if (release.await(10, TimeUnit.SECONDS)) return;
                    } catch (InterruptedException ignored) {
                        // a call without a timeout does not stop for an interrupt either
                    }
                }
            }),
            RetryPolicies.of(RetryPolicy.defaultPolicy()),
            new Observers(null),
            clock,
            status,
            true,
            true,
            true,
            FailureDetail.FULL);

    @AfterEach
    void releaseWorker() throws InterruptedException {
        release.countDown();
        if (worker != null) worker.join(5_000);
    }

    @Test
    void anAttemptPastTheDefaultMaxRunTimeIsCancelledThenAbandonedOnce() throws Exception {
        var processor = processor(DEFAULT_MAX_RUN_TIME);
        long id = startStuckAttempt(processor, TaskRequest.of(TYPE).build());

        clock.advance(DEFAULT_MAX_RUN_TIME.plusMillis(1));
        processor.renewOnce();

        assertThat(context.get().cancelRequested()).isTrue();
        var renewedUntil = store.getTask(id).orElseThrow().leaseUntil();
        assertThat(renewedUntil).isEqualTo(clock.instant().plus(LEASE));
        assertThat(reservations.abandoned()).isZero();

        clock.advance(RENEW_INTERVAL);
        processor.renewOnce();
        clock.advance(RENEW_INTERVAL);
        processor.renewOnce();

        assertThat(store.getTask(id).orElseThrow().leaseUntil()).isEqualTo(renewedUntil);
        assertThat(reservations.abandoned()).isEqualTo(1);
        assertThat(status.issues())
                .filteredOn(i -> i.kind().equals("attempt-abandoned"))
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.taskId()).isEqualTo(id);
                    assertThat(i.detail()).contains("1 abandoned attempts hold slots");
                });

        release.countDown();
        worker.join(5_000);

        assertThat(reservations.abandoned()).isZero();
        assertThat(running).isEmpty();
    }

    @Test
    void theTasksOwnMaxRunTimeWinsOverTheDefault() throws Exception {
        var processor = processor(Duration.ofMinutes(1));
        long id = startStuckAttempt(
                processor,
                TaskRequest.of(TYPE).maxRunTime(Duration.ofSeconds(1)).build());

        clock.advance(Duration.ofMillis(1_001));
        processor.renewOnce();

        assertThat(context.get().cancelRequested()).isTrue();

        clock.advance(RENEW_INTERVAL);
        processor.renewOnce();

        assertThat(reservations.abandoned()).isEqualTo(1);
        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void anAttemptWithinItsMaxRunTimeKeepsItsLease() throws Exception {
        var processor = processor(DEFAULT_MAX_RUN_TIME);
        long id = startStuckAttempt(processor, TaskRequest.of(TYPE).build());

        clock.advance(DEFAULT_MAX_RUN_TIME.minusMillis(1));
        processor.renewOnce();

        assertThat(context.get().cancelRequested()).isFalse();
        assertThat(store.getTask(id).orElseThrow().leaseUntil())
                .isEqualTo(clock.instant().plus(LEASE));
        assertThat(reservations.abandoned()).isZero();
    }

    private long startStuckAttempt(TaskProcessorService processor, TaskRequest<Void> request) throws Exception {
        long id = client.enqueue(request).id();
        var method = TaskProcessorService.class.getDeclaredMethod("processOnce");
        method.setAccessible(true);
        method.invoke(processor);
        worker = new Thread(executor.task, "overrun-test-worker");
        worker.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        return id;
    }

    private TaskProcessorService processor(Duration defaultMaxRunTime) {
        return new TaskProcessorService.Builder()
                .group(new ThreadGroup("overrun-test"))
                .threadName("overrun-test")
                .settings(settings)
                .running(running)
                .reservations(reservations)
                .executor(executor)
                .queueIds(List.of(queueId))
                .knownTypeIds(Set.of(typeId))
                .claimBatchSize(1)
                .idlePollInterval(Duration.ofMillis(1))
                .lease(LEASE)
                .defaultMaxRunTime(defaultMaxRunTime)
                .workerId(1)
                .create();
    }

    private static final class CapturingExecutor implements BackgroundExecutor {
        private Runnable task;
        private int available = 1;

        @Override
        public int reserve(int max, Duration wait) {
            int given = Math.min(max, available);
            available -= given;
            return given;
        }

        @Override
        public void release(int slots) {
            available += slots;
        }

        @Override
        public void execute(Runnable next) {
            task = next;
        }

        @Override
        public int activeCount() {
            return task == null ? 0 : 1;
        }

        @Override
        public void shutdown() {}

        @Override
        public boolean awaitTermination(Duration timeout) {
            return true;
        }
    }
}
