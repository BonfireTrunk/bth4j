package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SlotGateTest {

    private static final Duration SHORT = Duration.ofMillis(50);

    @Test
    void reserveTakesASlotAndAnInFlightPermitForEachTask() throws InterruptedException {
        var slots = new Semaphore(3);
        var gate = new SlotGate(slots, 6);

        assertThat(gate.reserve(5, SHORT)).isEqualTo(3);
        assertThat(slots.availablePermits()).isZero();

        gate.release(2);
        assertThat(slots.availablePermits()).isEqualTo(2);
        assertThat(gate.reserve(5, SHORT)).isEqualTo(2);
    }

    @Test
    void aTaskThatFreesItsSlotLetsTheNextOneReserveWhileItStillRuns() throws Exception {
        var gate = new SlotGate(new Semaphore(1), 2);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);
        var slotFreed = new CountDownLatch(1);
        var finish = new CountDownLatch(1);

        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            pool.submit(() -> gate.run(freeSlot -> {
                freeSlot.run();
                slotFreed.countDown();
                await(finish);
            }));
            assertThat(slotFreed.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(gate.reserve(1, Duration.ofSeconds(10))).isEqualTo(1);
            finish.countDown();
        }
    }

    @Test
    void freeingASlotTwiceGivesItBackOnce() throws Exception {
        var slots = new Semaphore(1);
        var gate = new SlotGate(slots, 2);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);

        gate.run(freeSlot -> {
            freeSlot.run();
            freeSlot.run();
        });

        assertThat(slots.availablePermits()).isEqualTo(1);
        assertThat(gate.reserve(2, SHORT)).isEqualTo(1);
    }

    @Test
    void tasksWritingTheirOutcomeHoldBackReservesOnceTheInFlightBoundIsReached() throws Exception {
        var slots = new Semaphore(1);
        var gate = new SlotGate(slots, 2);
        var freed = new CountDownLatch(2);
        var finish = new CountDownLatch(1);

        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var i = 0; i < 2; i++) {
                assertThat(gate.reserve(1, Duration.ofSeconds(10))).isEqualTo(1);
                pool.submit(() -> gate.run(freeSlot -> {
                    freeSlot.run();
                    freed.countDown();
                    await(finish);
                }));
            }
            assertThat(freed.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(slots.availablePermits()).isEqualTo(1);

            assertThat(gate.reserve(1, SHORT)).isZero();
            assertThat(slots.availablePermits()).isEqualTo(1);

            finish.countDown();
            assertThat(gate.reserve(1, Duration.ofSeconds(10))).isEqualTo(1);
        }
    }

    @Test
    void aTaskThatThrowsGivesBackItsSlotAndItsInFlightPermit() throws InterruptedException {
        var slots = new Semaphore(1);
        var gate = new SlotGate(slots, 1);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);

        assertThatThrownBy(() -> gate.run(freeSlot -> {
                    throw new IllegalStateException("task failure");
                }))
                .hasMessage("task failure");

        assertThat(slots.availablePermits()).isEqualTo(1);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);
    }

    @Test
    void aReserveThatFindsNoSlotGivesBackTheInFlightPermitItTook() throws InterruptedException {
        var slots = new Semaphore(0);
        var gate = new SlotGate(slots, 1);

        assertThat(gate.reserve(1, SHORT)).isZero();

        slots.release();
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);
    }

    @Test
    void theInFlightBoundIsTwiceTheSlotLimitWithoutOverflow() {
        assertThat(SlotGate.inFlightFor(64)).isEqualTo(128);
        assertThat(SlotGate.inFlightFor(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
