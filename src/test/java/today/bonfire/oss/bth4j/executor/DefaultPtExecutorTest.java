package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class DefaultPtExecutorTest {

    private final DefaultPtExecutor executor = new DefaultPtExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdown();
    }

    @Test
    void poolIsFullWhenRunningAndQueuedTasksReachTheLimit() throws InterruptedException {
        int limit = executor.getCurrentMaxThreads();
        var release = new CountDownLatch(1);

        assertThat(executor.isPoolFull()).isFalse();
        // more tasks than the limit: the extra ones wait in the unbounded local queue
        for (int i = 0; i < limit + 2; i++) {
            executor.execute(() -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        DefaultVtExecutorTest.awaitTrue(executor::isPoolFull);

        release.countDown();
        DefaultVtExecutorTest.awaitTrue(() -> !executor.isPoolFull());
    }

    @Test
    void executeRunsTasksAndAwaitTerminationFinishes() throws InterruptedException {
        var latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            executor.execute(latch::countDown);
        }
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();
    }
}
