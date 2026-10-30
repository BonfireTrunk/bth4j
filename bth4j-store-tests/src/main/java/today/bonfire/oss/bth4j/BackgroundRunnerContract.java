package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.common.Random;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;
import today.bonfire.oss.bth4j.service.BackgroundRunner;
import today.bonfire.oss.bth4j.service.Task;
import today.bonfire.oss.bth4j.service.TaskOps;
import today.bonfire.oss.bth4j.service.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.store.TaskStore;

/**
 * Contract of a whole runner against a real store: enqueue, retry, dead tasks, recurring tasks, leases and
 * shutdown. A store module subclasses it and supplies its {@link TaskStore} through {@link #newStore(String)}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BackgroundRunnerContract {

    private BackgroundRunner backgroundRunner;
    private TaskOps taskOps;

    protected abstract TaskStore newStore(String namespace);

    @BeforeAll
    void beforeAll() {
        backgroundRunner = runnerBuilder("BTH1").build();
        taskOps = backgroundRunner.taskOps();
        backgroundRunner.run();
    }

    BackgroundRunner.Builder runnerBuilder(String namespace) {
        TaskProcessorRegistry registry = TestTaskProcessors.createTestRegistry();
        return new BackgroundRunner.Builder()
                .taskProcessorRegistry(registry)
                .eventParser(TestEvents.UNKNOWN::from)
                .taskRetryDelay(attempt -> Duration.ofSeconds(1))
                .idlePollInterval(Duration.ofMillis(500))
                .maintenanceCheckInterval(Duration.ofSeconds(1))
                .recurringTasksCheckInterval(Duration.ofSeconds(1))
                .leaseDuration(Duration.ofSeconds(1))
                .jsonMapper(new JsonMapperTest())
                .taskExecutor(new DefaultVtExecutor())
                .taskStore(newStore(namespace));
    }

    @AfterAll
    protected void afterAll() {
        if (backgroundRunner != null) {
            backgroundRunner.stopRunner();
        }
    }

    @BeforeEach
    void setUp() {
        TestTaskProcessors.resetCounters();
    }

    private static Task.Builder task(Event event) {
        return Task.Builder.newTask().accountId(Random.UIDBASE64()).event(event);
    }

    @Test
    @Timeout(30)
    void regularTaskRunsOnceAndIsRemoved() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        var t = task(TestEvents.REGULAR_TASK).build();

        assertThat(taskOps.enqueue(t)).isTrue();

        assertThat(TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS))
                .isTrue();
        assertThat(TestTaskProcessors.regularTaskExecutions.get()).isEqualTo(1);
        for (int i = 0; i < 50 && taskOps.getTask(t.uniqueId()).isPresent(); i++) Thread.sleep(100);
        assertThat(taskOps.getTask(t.uniqueId())).isEmpty();
        assertThat(taskOps.leasedCount()).isZero();
    }

    @Test
    @Timeout(30)
    void enqueueWakesAnIdleRunnerQuickly() throws InterruptedException {
        Thread.sleep(1500); // let the processor settle into its idle wait
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        long start = System.nanoTime();

        taskOps.enqueue(task(TestEvents.REGULAR_TASK).build());

        assertThat(TestTaskProcessors.completionLatch.await(5, TimeUnit.SECONDS))
                .isTrue();
        long millis = (System.nanoTime() - start) / 1_000_000;
        assertThat(millis).isLessThan(400);
    }

    @Test
    @Timeout(30)
    void duplicateIdIsRejected() {
        var t = task(TestEvents.DELAYED_TASK).executeAfter(600).build();
        assertThat(taskOps.enqueue(t)).isTrue();
        assertThat(taskOps.enqueue(t)).isFalse();
        assertThat(taskOps.scheduledCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @Timeout(30)
    void scheduledTaskRunsAtItsTime() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();
        taskOps.enqueue(task(TestEvents.DELAYED_TASK).executeAfter(2).build());

        assertThat(TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS))
                .isTrue();
        assertThat(System.currentTimeMillis() - startTime)
                .isGreaterThanOrEqualTo(2000)
                .isLessThan(4000);
        assertThat(TestTaskProcessors.delayedTaskExecutions.get()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    void taskDataReachesTheProcessor() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        taskOps.enqueue(task(TestEvents.COOL).build(), Map.of("name", "bth4j", "count", 3));

        assertThat(TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS))
                .isTrue();
        assertThat(TestTaskProcessors.receivedData.get())
                .containsEntry("name", "bth4j")
                .containsEntry("count", 3);
    }

    @Test
    @Timeout(120)
    void recurringTaskRunsEveryMinute() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        var definition =
                task(TestEvents.RECURRING_TASK).cronExpression("* * * * *").build();
        try {
            taskOps.addRecurringTask(definition);
            for (int i = 0; i < 100 && taskOps.scheduledCount() == 0; i++) Thread.sleep(100);
            assertThat(taskOps.scheduledCount()).isGreaterThanOrEqualTo(1);

            assertThat(TestTaskProcessors.completionLatch.await(70, TimeUnit.SECONDS))
                    .isTrue();
            assertThat(TestTaskProcessors.recurringTaskExecutions.get()).isBetween(1, 2);
        } finally {
            assertThat(taskOps.deleteRecurringTask(definition.uniqueId())).isTrue();
        }
    }

    @Test
    @Timeout(60)
    void failedTaskIsRetriedAfterTheDelay() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        long start = System.currentTimeMillis();
        taskOps.enqueue(task(TestEvents.RETRY_TASK).build());

        assertThat(TestTaskProcessors.completionLatch.await(30, TimeUnit.SECONDS))
                .isTrue();
        assertThat(TestTaskProcessors.retryTaskExecutions.get()).isEqualTo(3);
        // two failures, each retried after one second
        assertThat(System.currentTimeMillis() - start)
                .isGreaterThanOrEqualTo(2000)
                .isLessThan(10000);
    }

    @Test
    @Timeout(30)
    void tasksRunConcurrently() throws InterruptedException {
        int taskCount = 5;
        TestTaskProcessors.completionLatch = new CountDownLatch(taskCount);
        TestTaskProcessors.concurrencyLatch = new CountDownLatch(1);
        for (int i = 0; i < taskCount; i++) {
            taskOps.enqueue(task(TestEvents.CONCURRENT_TASK).build());
        }
        for (int i = 0; i < 100 && TestTaskProcessors.concurrentTaskExecutions.get() < taskCount; i++) {
            Thread.sleep(50);
        }
        assertThat(TestTaskProcessors.concurrentTaskExecutions.get()).isEqualTo(taskCount);
        TestTaskProcessors.concurrencyLatch.countDown();
        assertThat(TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS))
                .isTrue();
    }

    @Test
    @Timeout(60)
    void taskIsDeadAfterItsAttemptsAreExhausted() throws InterruptedException {
        var t = task(TestEvents.ALWAYS_FAILS).build(); // retryCount 1, so two attempts
        taskOps.enqueue(t);

        Task dead = null;
        for (int i = 0; i < 300 && dead == null; i++) {
            dead = taskOps.deadTasks(10).stream()
                    .filter(d -> d.uniqueId().equals(t.uniqueId()))
                    .findFirst()
                    .orElse(null);
            if (dead == null) Thread.sleep(100);
        }
        assertThat(dead).isNotNull();
        assertThat(dead.attempts()).isEqualTo(2);
        assertThat(dead.lastError()).contains("Simulated permanent failure");

        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        assertThat(taskOps.retryDeadTask(t.uniqueId())).isTrue();
        assertThat(taskOps.getTask(t.uniqueId()))
                .get()
                .extracting(Task::attempts)
                .isEqualTo(0);
    }

    @Test
    @Timeout(60)
    void longTaskKeepsItsLeaseAndRunsOnce() throws InterruptedException {
        TestTaskProcessors.longTaskMillis = 3500; // several lease durations of 1 s
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        var t = task(TestEvents.DEFAULT).build();
        taskOps.enqueue(t);

        assertThat(TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS))
                .isTrue();
        Thread.sleep(2500); // enough for maintenance to have reclaimed it if the lease had lapsed
        assertThat(TestTaskProcessors.longTaskExecutions.get()).isEqualTo(1);
        assertThat(taskOps.getTask(t.uniqueId())).isEmpty();
        assertThat(taskOps.deadTasks(100)).extracting(Task::uniqueId).doesNotContain(t.uniqueId());
    }

    @Test
    @Timeout(60)
    void stopRunnerWaitsForRunningTasksAndCompletesThem() throws InterruptedException {
        var runner =
                runnerBuilder("BTH2").shutdownTimeout(Duration.ofSeconds(15)).build();
        var ops = runner.taskOps();
        runner.run();
        TestTaskProcessors.longTaskMillis = 2500;
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        var t = task(TestEvents.DEFAULT).build();
        ops.enqueue(t);
        for (int i = 0; i < 100 && runner.runningTasks() == 0; i++) Thread.sleep(50);
        assertThat(runner.runningTasks()).isEqualTo(1);

        long start = System.currentTimeMillis();
        runner.stopRunner();

        assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(1500);
        assertThat(TestTaskProcessors.completionLatch.getCount()).isZero();
        assertThat(ops.getTask(t.uniqueId())).isEmpty();
        assertThat(ops.leasedCount()).isZero();
    }
}
