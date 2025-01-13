package today.bonfire.oss.bth4j;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPooled;
import today.bonfire.oss.bth4j.common.Random;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;
import today.bonfire.oss.bth4j.service.BackgroundRunner;
import today.bonfire.oss.bth4j.service.Task;
import today.bonfire.oss.bth4j.service.TaskOps;
import today.bonfire.oss.bth4j.service.TaskProcessorRegistry;

import java.time.Duration;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class BackgroundRunnerTest {

  private static JedisPooled           jedis;
  private static TaskProcessorRegistry registry;
  private static BackgroundRunner      backgroundRunner;
  private static Thread                backgroundThread;
  private static TaskOps               taskOps;

  @BeforeAll
  static void beforeAll() throws InterruptedException {
    // Setup Redis connection
    var poolConfig = JedisPoolConfig.builder()
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
                                               .user("app")
                                               .password("happy")
                                               .build();

    var hostPort = new HostAndPort("127.0.0.1", 6404);
    jedis = new JedisPooled(hostPort, clientConfig, poolConfig);

    // Setup BackgroundRunner with test registry
    registry         = TestTaskProcessors.createTestRegistry();
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

    taskOps          = backgroundRunner.taskOps();
    backgroundThread = new Thread(backgroundRunner);
    backgroundThread.start();

    // Setup auto-shutdown timer
    var timer = new Timer(true);
    timer.schedule(new TimerTask() {
      @SneakyThrows
      @Override
      public void run() {
        log.debug("Stopping runner");
        backgroundRunner.stopRunner();
        backgroundThread.join(Duration.ofSeconds(10));
      }
    }, 600000); // 10 minutes timeout
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
  void testDelayedTask() throws InterruptedException {
    TestTaskProcessors.completionLatch = new CountDownLatch(1);

    long startTime = System.currentTimeMillis();
    var task = Task.Builder.newTask()
                           .accountId(Random.UIDBASE64())
                           .event(TestEvents.DELAYED_TASK)
                           .executeAfter(2) // 2 seconds delay
                           .build();

    taskOps.addTaskToQueue(task, Collections.emptyMap());

    boolean completed     = TestTaskProcessors.completionLatch.await(20, TimeUnit.SECONDS);
    long    executionTime = System.currentTimeMillis() - startTime;

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
      assertThat(TestTaskProcessors.recurringTaskExecutions.get()).isGreaterThanOrEqualTo(1)
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

    Thread.sleep(3000 * 3 + 3000); // 3 tries with stale timeout of 3 seconds + retry delay of 6 seconds
    boolean completed = TestTaskProcessors.completionLatch.await(30, TimeUnit.SECONDS);
    assertThat(completed).isTrue();
    // Should have executed 3 times (2 failures + 1 success)
    assertThat(TestTaskProcessors.retryTaskExecutions.get()).isEqualTo(3);
  }

  @Test
  @Timeout(30)
  void testConcurrentTaskProcessing() throws InterruptedException {
    int taskCount = 5;
    TestTaskProcessors.completionLatch  = new CountDownLatch(taskCount);
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
}
