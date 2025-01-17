package today.bonfire.oss.bth4j.examples;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPooled;
import today.bonfire.oss.bth4j.JsonMapperTest;
import today.bonfire.oss.bth4j.TasksConfigExample;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.Random;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;
import today.bonfire.oss.bth4j.service.BackgroundRunner;
import today.bonfire.oss.bth4j.service.Task;
import today.bonfire.oss.bth4j.service.TaskOps;
import today.bonfire.oss.bth4j.service.TaskProcessorRegistry;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

@Slf4j
class BackgroundRunnerExample {

  private static JedisPooled           jedis;
  private static TaskProcessorRegistry registry = TasksConfigExample.newTaskProcessorRegistry();
  private static Thread                th;
  private static TaskOps               taskOps;

  @BeforeAll
  static void beforeAll() throws InterruptedException {
    var builder = JedisPoolConfig.builder();
    builder.maxPoolSize(1000)
           .minPoolSize(1)
           .testOnBorrow(true)
           .testWhileIdle(true)
           .testOnReturn(false)
           .objEvictionTimeout(Duration.ofSeconds(5))
           .durationBetweenEvictionsRuns(Duration.ofSeconds(5))
           .waitingForObjectTimeout(Duration.ofSeconds(2))
           .abandonedTimeout(Duration.ofSeconds(2))
           .durationBetweenAbandonCheckRuns(Duration.ofSeconds(2));


    var clientConfig = DefaultJedisClientConfig.builder()
                                               .resp3()
                                               .timeoutMillis(1000)
                                               .clientName("bth4j-test")
                                               .user("app")
                                               .password("happy")
                                               .build();
    var hostPort = new HostAndPort("127.0.0.1", 6404);
    jedis = new JedisPooled(hostPort, clientConfig, builder.build());
    var bg = new BackgroundRunner.Builder().taskProcessorRegistry(registry)
                                           .eventParser(TestEvents.UNKNOWN::from)
                                           .taskRetryDelay(Integer::valueOf)
                                           .taskProcessorQueueCheckInterval(Duration.ofMillis(10))
                                           .maintenanceCheckInterval(Duration.ofSeconds(10))
                                           .staleTaskTimeout(Duration.ofSeconds(10))
                                           .jsonMapper(new JsonMapperTest())
                                           .jedisClient(jedis)
                                           .namespacePrefix("cool")
                                           .taskExecutor(new DefaultVtExecutor())
                                           .build();
    taskOps = bg.taskOps();
    th      = new Thread(bg);
    var timer = new Timer(true);
    timer.schedule(new TimerTask() {
      @SneakyThrows @Override
      public void run() {
        log.debug("Stopping runner");
        bg.stopRunner();
        th.join(Duration.ofSeconds(10));
      }
    }, 600000);
  }

  @AfterAll
  static void afterAll() {
    jedis.close();
  }

  @BeforeEach
  void setUp() {}

  @AfterEach
  void tearDown() {}

  @Test
  void test() throws InterruptedException {

    var cronSet = Arrays.asList(
        "1 * * * *", "0 0 1,15 * 3",
        "5 0 * 8 *", "15 14 1 * *",
        "0 22 * * 1-5", "23 0-22 * * *",
        "5 4 * * sun", "0 0,12 1 2 *");

    CountDownLatch latch = new CountDownLatch(100);
    for (int j = 0; j < 100; j++) {
      Thread.startVirtualThread(() -> {
        try {
          for (int i = 0; i < 1000; i++) {
            var t = Task.Builder.newTask()
                                .accountId(Random.UIDBASE64())
                                .event(TestEvents.COOL)
                                .executeAfter(5)
                                .build();
            taskOps.addTaskToQueue(t, Collections.emptyMap());
          }
        } finally {
          latch.countDown(); // Decrement the latch count when the thread completes
        }
      });
    }
    latch.await();
    th.start();
    th.join();
  }
}
