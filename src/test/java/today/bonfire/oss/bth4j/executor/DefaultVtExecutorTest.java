package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class DefaultVtExecutorTest {

    private final List<DefaultVtExecutor> created = new ArrayList<>();

    private DefaultVtExecutor newExecutor() {
        var executor = new DefaultVtExecutor();
        created.add(executor);
        return executor;
    }

    @AfterEach
    void tearDown() {
        created.forEach(DefaultVtExecutor::shutdown);
    }

    static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10 seconds");
            }
            Thread.sleep(5);
        }
    }

    @Test
    void executeRunsTasksAndReleasesPermits() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.getAvailablePermits();
        int tasks = Math.min(permits, 4);
        var latch = new CountDownLatch(tasks);

        for (int i = 0; i < tasks; i++) {
            executor.execute(latch::countDown);
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        awaitTrue(() -> executor.getActiveTaskCount() == 0 && executor.getAvailablePermits() >= permits);
        assertThat(executor.isPoolFull()).isFalse();
    }

    @Test
    void poolIsFullWhenAllPermitsAreInUseAndFurtherTasksAreRejected() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.getAvailablePermits();
        var started = new CountDownLatch(permits);
        var release = new CountDownLatch(1);

        for (int i = 0; i < permits; i++) {
            executor.execute(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.isPoolFull()).isTrue();
        assertThat(executor.getActiveTaskCount()).isEqualTo(permits);
        assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);

        release.countDown();
        awaitTrue(() -> !executor.isPoolFull());
        awaitTrue(() -> executor.getActiveTaskCount() == 0);
        // accepting work again
        var ran = new CountDownLatch(1);
        executor.execute(ran::countDown);
        assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void failedTaskReleasesItsPermit() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.getAvailablePermits();

        executor.execute(() -> {
            throw new IllegalStateException("simulated task failure");
        });

        awaitTrue(() -> executor.getActiveTaskCount() == 0 && executor.getAvailablePermits() >= permits);
    }

    @Test
    void rejectedExecutionAfterShutdownDoesNotLeakPermits() {
        var executor = newExecutor();
        int permits = executor.getAvailablePermits();
        executor.shutdown();

        assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);
        assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);

        assertThat(executor.getAvailablePermits()).isEqualTo(permits);
        assertThat(executor.getActiveTaskCount()).isZero();
    }

    @Test
    void awaitTerminationWaitsForRunningTasks() throws InterruptedException {
        var executor = newExecutor();
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);

        executor.execute(() -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofMillis(100))).isFalse();

        release.countDown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();
    }

    @Test
    void awaitTerminationReturnsTrueWhenIdle() throws InterruptedException {
        var executor = newExecutor();
        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void initialLimitIsTheNumberOfProcessors() {
        var executor = newExecutor();
        assertThat(executor.getCurrentMaxThreads())
                .isEqualTo(Runtime.getRuntime().availableProcessors());
        assertThat(executor.getAvailablePermits()).isEqualTo(executor.getCurrentMaxThreads());
    }
}
