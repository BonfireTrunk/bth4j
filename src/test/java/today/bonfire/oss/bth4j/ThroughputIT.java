package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * Measures how fast a single runner drains a burst of tasks. The numbers are logged; the assertion is a loose
 * lower bound so that the test does not fail on a slow machine.
 */
@Testcontainers
class ThroughputIT {
    private static final Logger log = LoggerFactory.getLogger(ThroughputIT.class);
    private static final int TASKS = 5120;

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static RedisClient jedis;
    private static BackgroundRunner runner;

    @BeforeAll
    static void beforeAll() {
        jedis = RedisClient.builder()
                .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                .clientConfig(DefaultJedisClientConfig.builder()
                        .resp3()
                        .timeoutMillis(2000)
                        .build())
                .poolConfig(ConnectionPoolConfig.builder()
                        .maxPoolSize(64)
                        .waitingForObjectTimeout(Duration.ofSeconds(5))
                        .build())
                .build();
        runner = new BackgroundRunner.Builder()
                .taskProcessorRegistry(TestTaskProcessors.createTestRegistry())
                .eventParser(TestEvents.UNKNOWN::from)
                .jedisClient(jedis)
                .taskExecutor(new DefaultVtExecutor())
                .claimBatchSize(64)
                .namespacePrefix("PERF")
                .build();
        runner.run();
    }

    @AfterAll
    static void afterAll() {
        runner.stopRunner();
        jedis.close();
    }

    @Test
    @Timeout(120)
    void drainsABurstOfTasks() throws InterruptedException {
        var taskOps = runner.taskOps();
        TestTaskProcessors.resetCounters();
        TestTaskProcessors.completionLatch = new CountDownLatch(TASKS);

        var enqueueStart = System.nanoTime();
        var producers = new ArrayList<Thread>();
        for (int p = 0; p < 16; p++) {
            var producer = Thread.ofVirtual().unstarted(() -> {
                for (int i = 0; i < TASKS / 16; i++) {
                    taskOps.enqueue(Task.Builder.newTask()
                            .accountId(Random.UIDBASE64())
                            .event(TestEvents.REGULAR_TASK)
                            .build());
                }
            });
            producer.start();
            producers.add(producer);
        }
        for (var producer : producers) producer.join();
        var enqueueMillis = (System.nanoTime() - enqueueStart) / 1_000_000;

        assertThat(TestTaskProcessors.completionLatch.await(90, TimeUnit.SECONDS))
                .isTrue();
        var totalMillis = (System.nanoTime() - enqueueStart) / 1_000_000;

        log.info(
                "Enqueued {} tasks in {} ms ({} tasks/s), all processed after {} ms ({} tasks/s end to end)",
                TASKS,
                enqueueMillis,
                TASKS * 1000L / Math.max(1, enqueueMillis),
                totalMillis,
                TASKS * 1000L / Math.max(1, totalMillis));
        assertThat(TestTaskProcessors.regularTaskExecutions.get()).isEqualTo(TASKS);
        for (int i = 0; i < 100 && taskOps.leasedCount() > 0; i++) Thread.sleep(50);
        assertThat(taskOps.leasedCount()).isZero();
        assertThat(TASKS * 1000L / Math.max(1, totalMillis)).isGreaterThan(200);
    }
}
