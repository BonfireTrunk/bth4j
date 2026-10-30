package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class DefaultPtExecutorTest {

    private static final Duration SHORT = Duration.ofMillis(50);

    private final DefaultPtExecutor executor = new DefaultPtExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdown();
    }

    private static Runnable blockOn(CountDownLatch started, CountDownLatch release) {
        return () -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    @Test
    void reserveReturnsBetweenOneAndMaxWhenSlotsAreFree() throws InterruptedException {
        int limit = executor.getCurrentMaxThreads();

        int reserved = executor.reserve(limit + 5, Duration.ofSeconds(1));

        assertThat(reserved).isBetween(1, limit);
    }

    @Test
    void reserveReturnsZeroWhenAllSlotsAreReservedAndReleaseMakesThemAvailable() throws InterruptedException {
        int limit = executor.getCurrentMaxThreads();
        assertThat(executor.reserve(limit, Duration.ofSeconds(1))).isEqualTo(limit);

        assertThat(executor.reserve(1, SHORT)).isZero();

        executor.release(limit);
        assertThat(executor.reserve(limit, Duration.ofSeconds(1))).isEqualTo(limit);
    }

    @Test
    void executeConsumesOneReservedSlotAndFreesItWhenTheTaskFinishes() throws InterruptedException {
        int limit = executor.getCurrentMaxThreads();
        assertThat(executor.reserve(limit, Duration.ofSeconds(1))).isEqualTo(limit);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);

        executor.execute(blockOn(started, release));

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.reserve(1, SHORT)).isZero();
        release.countDown();
        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);
    }

    @Test
    void activeCountReflectsARunningTask() throws InterruptedException {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);

        executor.execute(blockOn(started, release));

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.activeCount()).isEqualTo(1);
        release.countDown();
        DefaultVtExecutorTest.awaitTrue(() -> executor.activeCount() == 0);
    }

    @Test
    void executeAfterShutdownIsRejectedAndFreesTheReservedSlot() throws InterruptedException {
        int limit = executor.getCurrentMaxThreads();
        assertThat(executor.reserve(limit, Duration.ofSeconds(1))).isEqualTo(limit);
        executor.shutdown();

        assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);

        assertThat(executor.reserve(1, SHORT)).isEqualTo(1);
        assertThat(executor.activeCount()).isZero();
    }

    @Test
    void executeRunsTasksAndAwaitTerminationFinishes() throws InterruptedException {
        int tasks = executor.reserve(3, Duration.ofSeconds(1));
        assertThat(tasks).isPositive();
        var latch = new CountDownLatch(tasks);
        for (int i = 0; i < tasks; i++) {
            executor.execute(latch::countDown);
        }
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();
    }
}
