package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SlotGateTest {

    private static final Duration SHORT = Duration.ofMillis(50);

    @Test
    void reserveTakesASlotAndAnInFlightPermitForEachTask() throws InterruptedException {
        var gate = new SlotGate(3);

        assertThat(gate.reserve(5, SHORT)).isEqualTo(3);
        assertThat(gate.availableSlots()).isZero();
        assertThat(gate.availableInFlight()).isEqualTo(3);

        gate.release(2);
        assertThat(gate.availableSlots()).isEqualTo(2);
        assertThat(gate.availableInFlight()).isEqualTo(5);
        assertThat(gate.reserve(5, SHORT)).isEqualTo(2);
    }

    @Test
    void aTaskThatFreesItsSlotLetsTheNextOneReserveWhileItStillRuns() throws Exception {
        var gate = new SlotGate(1);
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
        var gate = new SlotGate(1);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);

        gate.run(freeSlot -> {
            freeSlot.run();
            freeSlot.run();
        });

        assertThat(gate.availableSlots()).isEqualTo(1);
        assertThat(gate.availableInFlight()).isEqualTo(2);
        assertThat(gate.reserve(2, SHORT)).isEqualTo(1);
    }

    @Test
    void tasksWritingTheirOutcomeHoldBackReservesOnceTheInFlightBoundIsReached() throws Exception {
        var gate = new SlotGate(1);
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
            assertThat(gate.availableSlots()).isEqualTo(1);
            assertThat(gate.usable()).isZero();

            assertThat(gate.reserve(1, SHORT)).isZero();
            assertThat(gate.availableSlots()).isEqualTo(1);

            finish.countDown();
            assertThat(gate.reserve(1, Duration.ofSeconds(10))).isEqualTo(1);
        }
    }

    @Test
    void aTaskThatThrowsGivesBackItsSlotAndItsInFlightPermit() throws InterruptedException {
        var gate = new SlotGate(1);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);

        assertThatThrownBy(() -> gate.run(freeSlot -> {
                    throw new IllegalStateException("task failure");
                }))
                .hasMessage("task failure");

        assertThat(gate.availableSlots()).isEqualTo(1);
        assertThat(gate.availableInFlight()).isEqualTo(2);
    }

    @Test
    void aReserveThatFindsNoSlotGivesBackTheInFlightPermitItTook() throws InterruptedException {
        var gate = new SlotGate(1);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);

        assertThat(gate.reserve(1, SHORT)).isZero();

        assertThat(gate.availableInFlight()).isEqualTo(1);
    }

    @Test
    void aReserveInterruptedWhileWaitingForASlotGivesBackTheInFlightPermitItTook() throws Exception {
        var gate = new SlotGate(1);
        assertThat(gate.reserve(1, SHORT)).isEqualTo(1);
        var thrown = new AtomicReference<Throwable>();

        var reserver = Thread.ofVirtual().start(() -> {
            try {
                gate.reserve(1, Duration.ofSeconds(20));
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        while (gate.availableInFlight() != 0) Thread.onSpinWait();
        reserver.interrupt();
        reserver.join(10_000);

        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        assertThat(gate.availableInFlight()).isEqualTo(1);
        assertThat(gate.availableSlots()).isZero();
    }

    @Test
    void resizingMovesTheSlotLimitAndTheInFlightBoundTogether() throws InterruptedException {
        var gate = new SlotGate(4);
        assertThat(gate.reserve(3, SHORT)).isEqualTo(3);

        gate.resize(4, 2);
        assertThat(gate.availableSlots()).isEqualTo(-1);
        assertThat(gate.availableInFlight()).isEqualTo(1);
        assertThat(gate.reserve(1, SHORT)).isZero();

        gate.resize(2, 6);
        assertThat(gate.availableSlots()).isEqualTo(3);
        assertThat(gate.availableInFlight()).isEqualTo(9);

        gate.release(3);
        assertThat(gate.availableSlots()).isEqualTo(6);
        assertThat(gate.availableInFlight()).isEqualTo(12);
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
