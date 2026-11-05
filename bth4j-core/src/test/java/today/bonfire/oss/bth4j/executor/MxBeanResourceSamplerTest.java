package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.MemoryUsage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.MemorySignal;

class MxBeanResourceSamplerTest {
    private final AtomicReference<Double> cpu = new AtomicReference<>(0.4);
    private final AtomicReference<MemoryUsage> heap = new AtomicReference<>(usage(20));
    private final AtomicLong free = new AtomicLong(30);
    private final AtomicLong total = new AtomicLong(100);

    private static MemoryUsage usage(long used) {
        return new MemoryUsage(0, used, 100, 100);
    }

    private MxBeanResourceSampler sampler(MemorySignal signal) {
        return new MxBeanResourceSampler(signal, cpu::get, heap::get, free::get, total::get);
    }

    private static long seconds(long seconds) {
        return TimeUnit.SECONDS.toNanos(seconds);
    }

    @Test
    void invalidCpuIsUnknownIncludingUnsupportedAndThrowingSources() {
        var sampler = sampler(MemorySignal.HEAP);
        for (double value : new double[] {-1, Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1.01}) {
            cpu.set(value);
            assertThat(sampler.sample(0).cpu()).isNaN();
        }
        cpu.set(0.0);
        assertThat(sampler.sample(0).cpu()).isZero();
        cpu.set(1.0);
        assertThat(sampler.sample(0).cpu()).isEqualTo(1);
        var unavailable = new MxBeanResourceSampler(
                MemorySignal.HEAP,
                () -> {
                    throw new UnsupportedOperationException();
                },
                heap::get,
                free::get,
                total::get);
        assertThat(unavailable.sample(0).cpu()).isNaN();
        assertThat(unavailable.sample(0).memory()).isEqualTo(0.2);
    }

    @Test
    void heapUsesLowestInTheLastTwoSecondsAndExpiresAtTheBoundary() {
        var sampler = sampler(MemorySignal.HEAP);
        assertThat(sampler.sample(0).memory()).isEqualTo(0.2);
        heap.set(usage(90));
        assertThat(sampler.sample(seconds(1)).memory()).isEqualTo(0.2);
        assertThat(sampler.sample(seconds(2)).memory()).isEqualTo(0.9);
        heap.set(usage(10));
        assertThat(sampler.sample(seconds(3)).memory()).isEqualTo(0.1);
        heap.set(usage(80));
        assertThat(sampler.sample(seconds(30)).memory()).isEqualTo(0.8);
    }

    @Test
    void undefinedHeapMaxUsesCommittedAndZeroOrUnavailableMemoryIsUnknownImmediately() {
        var sampler = sampler(MemorySignal.HEAP);
        heap.set(new MemoryUsage(0, 50, 100, -1));
        assertThat(sampler.sample(0).memory()).isEqualTo(0.5);
        heap.set(new MemoryUsage(0, 0, 0, -1));
        assertThat(sampler.sample(1).memory()).isNaN();
        heap.set(null);
        assertThat(sampler.sample(2).memory()).isNaN();
    }

    @Test
    void combinedMemoryTakesTheGreaterOfIndependentWindowMinima() {
        var sampler = sampler(MemorySignal.HEAP_AND_SYSTEM);
        assertThat(sampler.sample(0).memory()).isEqualTo(0.7);
        heap.set(usage(90));
        free.set(70);
        assertThat(sampler.sample(seconds(1)).memory()).isCloseTo(0.3, org.assertj.core.data.Offset.offset(0.00001));
        assertThat(sampler.sample(seconds(2)).memory()).isEqualTo(0.9);
    }

    @Test
    void combinedMemoryIsUnknownIfEitherComponentIsInvalid() {
        var sampler = sampler(MemorySignal.HEAP_AND_SYSTEM);
        for (long value : new long[] {-1, 101}) {
            free.set(value);
            assertThat(sampler.sample(0).memory()).isNaN();
        }
        free.set(30);
        for (long value : new long[] {-1, 0}) {
            total.set(value);
            assertThat(sampler.sample(0).memory()).isNaN();
        }
        total.set(100);
        heap.set(null);
        assertThat(sampler.sample(0).memory()).isNaN();
    }

    @Test
    void heapModeDoesNotReadSystemMemory() {
        var sampler = new MxBeanResourceSampler(
                MemorySignal.HEAP,
                cpu::get,
                heap::get,
                () -> {
                    throw new AssertionError("system memory must be opt-in");
                },
                total::get);
        assertThat(sampler.sample(0).memory()).isEqualTo(0.2);
    }
}
