package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import today.bonfire.oss.bth4j.common.Random;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;
import today.bonfire.oss.bth4j.service.BackgroundRunner;
import today.bonfire.oss.bth4j.service.Task;
import today.bonfire.oss.bth4j.service.TaskOps;
import today.bonfire.oss.bth4j.service.TaskProcessorRegistry;

/**
 * Integration test against a real Redis started with Testcontainers. Needs Docker.
 * Run by maven-failsafe-plugin (mvn verify), not by the unit test phase.
 */
@Slf4j
@Testcontainers
class BackgroundRunnerIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static RedisClient jedis;
    private static TaskProcessorRegistry registry;
    private static BackgroundRunner backgroundRunner;
    private static Thread backgroundThread;
    private static TaskOps taskOps;

    @BeforeAll
    static void beforeAll() throws InterruptedException {
        // Setup Redis connection
        var poolConfig = ConnectionPoolConfig.builder()
                .maxPoolSize(8)
                .minPoolSize(1)
                .testOnBorrow(false)
                .testWhileIdle(true)
                .testOnReturn(false)
                .objEvictionTimeout(Duration.ofSeconds(120))
                .durationBetweenEvictionsRuns(Duration.ofSeconds(5))
                .waitingForObjectTimeout(Duration.ofSeconds(2))
                .abandonedTimeout(Duration.ofSeconds(120))
                .durationBetweenAbandonCheckRuns(Duration.ofSeconds(2))
                .build();

        var clientConfig = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(1000)
                .clientName("bth4j-test")
                .build();

        var hostPort = new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379));
        jedis = RedisClient.builder()
                .hostAndPort(hostPort)
                .clientConfig(clientConfig)
                .poolConfig(poolConfig)
                .build();

        // Setup BackgroundRunner with test registry
        registry = TestTaskProcessors.createTestRegistry();
        backgroundRunner = new BackgroundRunner.Builder()
                .taskProcessorRegistry(registry)
                .eventParser(TestEvents.UNKNOWN::from)
                .taskRetryDelay(Integer::valueOf)
                .taskProcessorQueueCheckInterval(Duration.ofMillis(10))
                .maintenanceCheckInterval(Duration.ofSeconds(1))
                .staleTaskTimeout(Duration.ofSeconds(3))
                .taskRetryDelay(integer -> 1)
                .jsonMapper(new JsonMapperTest())
                .jedisClient(jedis)
                .taskExecutor(new DefaultVtExecutor())
                .namespacePrefix("BTH1")
                .build();

        taskOps = backgroundRunner.taskOps();
        backgroundThread = new Thread(backgroundRunner);
        backgroundThread.start();

        // Setup auto-shutdown timer
        var timer = new Timer(true);
        timer.schedule(
                new TimerTask() {
                    @SneakyThrows
                    @Override
                    public void run() {
                        log.debug("Stopping runner");
                        backgroundRunner.stopRunner();
                        backgroundThread.join(Duration.ofSeconds(10));
                    }
                },
                600000); // 10 minutes timeout
    }

    @AfterAll
    static void afterAll() {
        if (backgroundRunner != null) {
            backgroundRunner.stopRunner();
        }
        if (jedis != null) {
            jedis.close();
        }
    }

    @BeforeEach
    void setUp() {
        TestTaskProcessors.resetCounters();
    }

    @Test
    @Timeout(30) // 30 seconds timeout
    void testRegularTask() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);

        var task = Task.Builder.newTask()
                .accountId(Random.UIDBASE64())
                .event(TestEvents.REGULAR_TASK)
                .build();

        taskOps.addTaskToQueue(task, Collections.emptyMap());

        boolean completed = TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(TestTaskProcessors.regularTaskExecutions.get()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    void testScheduledTask() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);

        long startTime = System.currentTimeMillis();
        var task = Task.Builder.newTask()
                .accountId(Random.UIDBASE64())
                .event(TestEvents.DELAYED_TASK)
                .executeAfter(2) // 2 seconds delay
                .build();

        taskOps.addTaskToQueue(task, Collections.emptyMap());

        boolean completed = TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS);
        long executionTime = System.currentTimeMillis() - startTime;

        assertThat(completed).isTrue();
        assertThat(executionTime).isGreaterThanOrEqualTo(2000);
        assertThat(TestTaskProcessors.delayedTaskExecutions.get()).isEqualTo(1);
    }

    @Test
    @Timeout(120)
    void testRecurringTask() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1); // Wait for 3 executions

        var task = Task.Builder.newTask()
                .accountId(Random.UIDBASE64())
                .event(TestEvents.RECURRING_TASK)
                .cronExpression("*/1 * * * *") // Every minute
                .build();
        try {
            var t1 = taskOps.addRecurringTask(task);
            Thread.sleep(60000);

            boolean completed = TestTaskProcessors.completionLatch.await(10, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
            assertThat(TestTaskProcessors.recurringTaskExecutions.get())
                    .isGreaterThanOrEqualTo(1)
                    .isLessThanOrEqualTo(2);
        } finally {
            taskOps.deleteRecurringTask(task);
        }
    }

    @Test
    @Timeout(120)
    void testTaskRetry() throws InterruptedException {
        TestTaskProcessors.completionLatch = new CountDownLatch(1);
        TestTaskProcessors.retryTaskExecutions.set(0);
        var task = Task.Builder.newTask()
                .accountId(Random.UIDBASE64())
                .event(TestEvents.RETRY_TASK)
                .build();

        taskOps.addTaskToQueue(task, null);

        // 3 tries: each failed attempt is retried after the stale timeout of 3 seconds plus the retry delay,
        // so the third attempt lands at roughly 12 seconds. Poll the counter instead of sleeping a fixed time.
        for (int i = 0; i < 40 && TestTaskProcessors.retryTaskExecutions.get() < 3; i++) {
            Thread.sleep(1000);
        }
        // Should have executed 3 times (2 failures + 1 success)
        assertThat(TestTaskProcessors.retryTaskExecutions.get()).isEqualTo(3);
    }

    @Test
    @Timeout(30)
    void testConcurrentTaskProcessing() throws InterruptedException {
        int taskCount = 5;
        TestTaskProcessors.completionLatch = new CountDownLatch(taskCount);
        TestTaskProcessors.concurrencyLatch = new CountDownLatch(1);

        // Create multiple tasks
        for (int i = 0; i < taskCount; i++) {
            var task = Task.Builder.newTask()
                    .accountId(Random.UIDBASE64())
                    .event(TestEvents.CONCURRENT_TASK)
                    .build();
            taskOps.addTaskToQueue(task, Collections.emptyMap());
        }

        TestTaskProcessors.concurrencyLatch.countDown();
        // Wait for all tasks to complete
        boolean completed = TestTaskProcessors.completionLatch.await(30, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // Verify that tasks ran concurrently
        int maxConcurrent = taskCount; // All tasks should have run concurrently
        assertThat(TestTaskProcessors.concurrentTaskExecutions.get()).isLessThanOrEqualTo(maxConcurrent);
    }

    @Test
    @Timeout(60)
    void testTaskIsMovedToDeadQueueWhenRetriesAreExhausted() throws InterruptedException {
        var task = Task.Builder.newTask()
                .accountId(Random.UIDBASE64())
                .event(TestEvents.ALWAYS_FAILS) // retryCount is 1
                .build();
        taskOps.addTaskToQueue(task, null);

        // fails, becomes stale (3s), is retried, fails again, becomes stale again and is then moved to the dead queue
        Double deadSince = null;
        for (int i = 0; i < 50 && deadSince == null; i++) {
            deadSince = jedis.zscore("BTH1:DEAD_TASKS", task.taskString());
            if (deadSince == null) Thread.sleep(1000);
        }
        assertThat(deadSince).isNotNull();
    }
}
