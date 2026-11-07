package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.Concurrency;

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

    private static final Duration SHORT = Duration.ofMillis(50);

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
    void executeRunsTasksAndReleasesPermits() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.availablePermits();
        int tasks = Math.min(permits, 4);
        assertThat(executor.reserve(tasks, Duration.ofSeconds(1))).isEqualTo(tasks);
        var latch = new CountDownLatch(tasks);

        for (int i = 0; i < tasks; i++) {
            executor.execute(latch::countDown);
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        awaitTrue(() -> executor.activeCount() == 0 && executor.availablePermits() >= permits);
    }

    @Test
    void reserveReturnsBetweenOneAndMaxWhenSlotsAreFree() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.availablePermits();

        int reserved = executor.reserve(permits + 5, Duration.ofSeconds(1));

        assertThat(reserved).isBetween(1, permits);
        assertThat(executor.availablePermits()).isEqualTo(permits - reserved);
    }

    @Test
    void reserveNeverReturnsMoreThanMax() throws InterruptedException {
        var executor = newExecutor();
        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);
    }

    @Test
    void reserveReturnsZeroWhenAllSlotsAreReservedAndReleaseMakesThemAvailable() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.availablePermits();
        assertThat(executor.reserve(permits, Duration.ofSeconds(1))).isEqualTo(permits);

        assertThat(executor.reserve(1, SHORT)).isZero();

        executor.release(permits);
        assertThat(executor.reserve(permits, Duration.ofSeconds(1))).isEqualTo(permits);
    }

    @Test
    void aTaskThatFreesItsSlotEarlyGivesItBackWhileItStillRuns() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.availablePermits();
        assertThat(executor.reserve(permits, Duration.ofSeconds(1))).isEqualTo(permits);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);

        executor.executeWithSlot(freeSlot -> {
            freeSlot.run();
            blockOn(started, release).run();
        });

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.activeCount()).isEqualTo(1);
        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);
        release.countDown();
    }

    @Test
    void executeConsumesOneReservedSlotAndFreesItWhenTheTaskFinishes() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.availablePermits();
        assertThat(executor.reserve(permits, Duration.ofSeconds(1))).isEqualTo(permits);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);

        executor.execute(blockOn(started, release));

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.reserve(1, SHORT)).isZero();
        release.countDown();
        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);
    }

    @Test
    void failedTaskReleasesItsSlot() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.availablePermits();
        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);

        executor.execute(() -> {
            throw new IllegalStateException("simulated task failure");
        });

        awaitTrue(() -> executor.activeCount() == 0 && executor.availablePermits() >= permits);
    }

    @Test
    void activeCountReflectsARunningTask() throws InterruptedException {
        var executor = newExecutor();
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);

        executor.execute(blockOn(started, release));

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.activeCount()).isEqualTo(1);
        release.countDown();
        awaitTrue(() -> executor.activeCount() == 0);
    }

    @Test
    void executeAfterShutdownIsRejectedAndFreesTheReservedSlot() throws InterruptedException {
        var executor = newExecutor();
        int permits = executor.availablePermits();
        assertThat(executor.reserve(permits, Duration.ofSeconds(1))).isEqualTo(permits);
        executor.shutdown();

        assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);

        assertThat(executor.availablePermits()).isEqualTo(1);
        assertThat(executor.reserve(1, SHORT)).isEqualTo(1);
        assertThat(executor.activeCount()).isZero();
    }

    @Test
    void awaitTerminationWaitsForRunningTasks() throws InterruptedException {
        var executor = newExecutor();
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);

        assertThat(executor.reserve(1, Duration.ofSeconds(1))).isEqualTo(1);
        executor.execute(blockOn(started, release));
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
        assertThat(executor.currentMaxThreads()).isEqualTo(Runtime.getRuntime().availableProcessors());
        assertThat(executor.availablePermits()).isEqualTo(executor.currentMaxThreads());
    }

    @Test
    void shrinkWithPermitDebtRecoversByProbingAndAnEmptyClaimStopsGrowth() throws Exception {
        var now = new AtomicLong();
        var sample = new AtomicReference<>(new ResourceSampler.Sample(0, 0.1));
        var executor = new DefaultVtExecutor(Concurrency.adaptive(2, 10), ignored -> sample.get(), now::get);
        created.add(executor);
        executor.adjustThreadCount();
        assertThat(executor.reserve(2, Duration.ZERO)).isEqualTo(2);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        executor.claimed(2, 2);
        executor.adjustThreadCount();
        assertThat(executor.limit()).hasValue(3);
        assertThat(executor.reserve(1, Duration.ZERO)).isEqualTo(1);
        sample.set(new ResourceSampler.Sample(0, 0.95));
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        executor.adjustThreadCount();
        assertThat(executor.currentMaxThreads()).isEqualTo(2);
        assertThat(executor.availablePermits()).isEqualTo(-1);
        assertThat(executor.reserve(1, Duration.ZERO)).isZero();
        sample.set(new ResourceSampler.Sample(0, 0.1));
        for (int i = 0; i < 12; i++) {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
            executor.adjustThreadCount();
        }
        assertThat(executor.currentMaxThreads()).isEqualTo(3);
        assertThat(executor.availablePermits()).isZero();
        for (int i = 0; i < 8; i++) {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
            executor.adjustThreadCount();
        }
        assertThat(executor.currentMaxThreads()).isEqualTo(4);
        assertThat(executor.availablePermits()).isEqualTo(1);
        assertThat(executor.reserve(1, Duration.ZERO)).isEqualTo(1);
        executor.claimed(1, 0);
        executor.release(1);
        for (int i = 0; i < 12; i++) {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
            executor.adjustThreadCount();
        }
        assertThat(executor.currentMaxThreads()).isEqualTo(4);
        assertThat(executor.availablePermits()).isEqualTo(1);
        executor.release(3);
        assertThat(executor.availablePermits()).isEqualTo(4);
    }

    @Test
    void runningTasksAreNotCancelledByAnEmergencyShrink() throws Exception {
        var now = new AtomicLong();
        var sample = new AtomicReference<>(new ResourceSampler.Sample(0, 0.1));
        var executor = new DefaultVtExecutor(Concurrency.adaptive(1, 3), ignored -> sample.get(), now::get);
        created.add(executor);
        executor.adjustThreadCount();
        assertThat(executor.reserve(1, Duration.ZERO)).isEqualTo(1);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        executor.claimed(1, 1);
        executor.adjustThreadCount();
        assertThat(executor.reserve(1, Duration.ZERO)).isEqualTo(1);
        var started = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        try {
            executor.execute(blockOn(started, release));
            executor.execute(blockOn(started, release));
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            sample.set(new ResourceSampler.Sample(0, 1));
            executor.adjustThreadCount();
            assertThat(executor.activeCount()).isEqualTo(2);
            assertThat(executor.availablePermits()).isEqualTo(-1);
        } finally {
            release.countDown();
        }
        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();
        assertThat(executor.availablePermits()).isEqualTo(1);
    }

    @Test
    void emptyPollsEveryHalfSecondDoNotPreventIdleReset() throws Exception {
        var now = new AtomicLong();
        var executor = new DefaultVtExecutor(
                Concurrency.adaptive(2, 10), ignored -> new ResourceSampler.Sample(0, 0.1), now::get);
        created.add(executor);
        executor.adjustThreadCount();
        assertThat(executor.reserve(2, Duration.ZERO)).isEqualTo(2);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        executor.claimed(2, 2);
        executor.adjustThreadCount();
        executor.release(2);
        assertThat(executor.currentMaxThreads()).isEqualTo(3);
        for (int i = 1; i <= 60; i++) {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
            int reserved = executor.reserve(32, Duration.ZERO);
            executor.claimed(reserved, 0);
            executor.release(reserved);
            executor.adjustThreadCount();
            assertThat(executor.currentMaxThreads()).isEqualTo(i < 60 ? 3 : 2);
        }
    }

    @Test
    void runningTasksPreventIdleResetWithoutFurtherClaims() throws Exception {
        var now = new AtomicLong();
        var executor = new DefaultVtExecutor(
                Concurrency.adaptive(1, 3), ignored -> new ResourceSampler.Sample(0, 0.1), now::get);
        created.add(executor);
        executor.adjustThreadCount();
        assertThat(executor.reserve(1, Duration.ZERO)).isEqualTo(1);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        executor.claimed(1, 1);
        executor.adjustThreadCount();
        assertThat(executor.currentMaxThreads()).isEqualTo(2);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try {
            executor.execute(blockOn(started, release));
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < 80; i++) {
                now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
                executor.adjustThreadCount();
                assertThat(executor.currentMaxThreads()).isEqualTo(2);
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void nonemptyClaimBetweenTicksRestartsIdleTimer() throws Exception {
        var now = new AtomicLong();
        var executor = new DefaultVtExecutor(
                Concurrency.adaptive(2, 10), ignored -> new ResourceSampler.Sample(0, 0.1), now::get);
        created.add(executor);
        executor.adjustThreadCount();
        assertThat(executor.reserve(2, Duration.ZERO)).isEqualTo(2);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        executor.claimed(2, 2);
        executor.adjustThreadCount();
        executor.release(2);
        for (int i = 0; i < 59; i++) {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
            executor.adjustThreadCount();
        }
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(250));
        int reserved = executor.reserve(2, Duration.ZERO);
        executor.claimed(reserved, 1);
        executor.release(reserved);
        for (int i = 0; i < 59; i++) {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
            executor.adjustThreadCount();
            assertThat(executor.currentMaxThreads()).isEqualTo(3);
        }
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        executor.adjustThreadCount();
        assertThat(executor.currentMaxThreads()).isEqualTo(2);
    }

    @Test
    void maxConcurrencyBelowMinimumIsClampedToMinimum() {
        var executor = new DefaultVtExecutor(4, 2);
        created.add(executor);
        assertThat(executor.currentMaxThreads()).isEqualTo(4);
    }
}
