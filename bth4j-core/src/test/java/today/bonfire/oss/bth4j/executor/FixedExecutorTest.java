package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class FixedExecutorTest {

    @Test
    void limitMustBePositive() {
        assertThatThrownBy(() -> new FixedExecutor(0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new FixedExecutor(3).limit()).isEqualTo(3);
    }

    @Test
    void reserveGivesAtMostMaxAndAtMostTheFreeSlots() throws InterruptedException {
        var executor = new FixedExecutor(5);

        assertThat(executor.reserve(3, Duration.ofMillis(10))).isEqualTo(3);
        assertThat(executor.reserve(10, Duration.ofMillis(10))).isEqualTo(2);
        executor.shutdown();
    }

    @Test
    void reserveReturnsZeroWhenNothingBecomesFree() throws InterruptedException {
        var executor = new FixedExecutor(2);
        assertThat(executor.reserve(2, Duration.ofMillis(10))).isEqualTo(2);

        assertThat(executor.reserve(1, Duration.ofMillis(50))).isZero();
        executor.shutdown();
    }

    @Test
    void releaseMakesSlotsAvailableAgain() throws InterruptedException {
        var executor = new FixedExecutor(2);
        assertThat(executor.reserve(2, Duration.ofMillis(10))).isEqualTo(2);

        executor.release(1);
        executor.release(0);

        assertThat(executor.reserve(2, Duration.ofMillis(10))).isEqualTo(1);
        executor.shutdown();
    }

    @Test
    void reserveWaitsForASlotThatIsReleasedLater() throws Exception {
        var executor = new FixedExecutor(1);
        assertThat(executor.reserve(1, Duration.ofMillis(10))).isEqualTo(1);
        var releaser = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            executor.release(1);
        });
        releaser.start();

        assertThat(executor.reserve(1, Duration.ofSeconds(10))).isEqualTo(1);
        releaser.join();
        executor.shutdown();
    }

    @Test
    void executeRunsTheTaskAndFreesItsSlotWhenDone() throws Exception {
        var executor = new FixedExecutor(1);
        var ran = new CountDownLatch(1);
        assertThat(executor.reserve(1, Duration.ofMillis(10))).isEqualTo(1);

        executor.execute(ran::countDown);

        assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.reserve(1, Duration.ofSeconds(10))).isEqualTo(1);
        executor.shutdown();
    }

    @Test
    void executeFreesTheSlotWhenTheTaskThrows() throws Exception {
        var executor = new FixedExecutor(1);
        assertThat(executor.reserve(1, Duration.ofMillis(10))).isEqualTo(1);

        executor.execute(() -> {
            throw new IllegalStateException("task failure");
        });

        assertThat(executor.reserve(1, Duration.ofSeconds(10))).isEqualTo(1);
        executor.shutdown();
    }

    @Test
    void neverRunsMoreThanTheLimitAtOnce() throws Exception {
        int limit = 3;
        var executor = new FixedExecutor(limit);
        var running = new AtomicInteger();
        var peak = new AtomicInteger();
        var finished = new CountDownLatch(20);

        for (int submitted = 0; submitted < 20; ) {
            int slots = executor.reserve(20 - submitted, Duration.ofSeconds(10));
            for (int i = 0; i < slots; i++, submitted++) {
                executor.execute(() -> {
                    peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    running.decrementAndGet();
                    finished.countDown();
                });
            }
        }

        assertThat(finished.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(peak.get()).isBetween(1, limit);
        executor.shutdown();
    }

    @Test
    void activeCountReflectsRunningTasks() throws Exception {
        var executor = new FixedExecutor(2);
        var started = new CountDownLatch(2);
        var gate = new CountDownLatch(1);
        assertThat(executor.reserve(2, Duration.ofMillis(10))).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            executor.execute(() -> {
                started.countDown();
                try {
                    gate.await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.activeCount()).isEqualTo(2);

        gate.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();
        assertThat(executor.activeCount()).isZero();
    }

    @Test
    void awaitTerminationWaitsForRunningTasks() throws Exception {
        var executor = new FixedExecutor(1);
        var gate = new CountDownLatch(1);
        var started = new CountDownLatch(1);
        assertThat(executor.reserve(1, Duration.ofMillis(10))).isEqualTo(1);
        executor.execute(() -> {
            started.countDown();
            try {
                gate.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        executor.shutdown();

        assertThat(executor.awaitTermination(Duration.ofMillis(50))).isFalse();
        gate.countDown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();
    }

    @Test
    void executeAfterShutdownIsRejectedAndFreesTheSlot() throws Exception {
        var executor = new FixedExecutor(1);
        assertThat(executor.reserve(1, Duration.ofMillis(10))).isEqualTo(1);
        executor.shutdown();

        assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);

        assertThat(executor.reserve(1, Duration.ofMillis(10))).isEqualTo(1);
    }
}
