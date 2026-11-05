package today.bonfire.oss.bth4j.executor;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import today.bonfire.oss.bth4j.MemorySignal;

final class MxBeanResourceSampler implements ResourceSampler {
    private final MemorySignal signal;
    private final DoubleSupplier cpu;
    private final Supplier<MemoryUsage> heap;
    private final LongSupplier free;
    private final LongSupplier total;
    private final Window heapWindow = new Window();
    private final Window systemWindow = new Window();

    MxBeanResourceSampler(MemorySignal signal) {
        this(signal, extendedOsBean());
    }

    private MxBeanResourceSampler(MemorySignal signal, com.sun.management.OperatingSystemMXBean extended) {
        this(
                signal,
                extended == null ? () -> Double.NaN : extended::getCpuLoad,
                ManagementFactory.getMemoryMXBean()::getHeapMemoryUsage,
                extended == null ? () -> -1 : extended::getFreeMemorySize,
                extended == null ? () -> -1 : extended::getTotalMemorySize);
    }

    private static com.sun.management.OperatingSystemMXBean extendedOsBean() {
        return ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean bean
                ? bean
                : null;
    }

    MxBeanResourceSampler(
            MemorySignal signal,
            DoubleSupplier cpu,
            Supplier<MemoryUsage> heap,
            LongSupplier free,
            LongSupplier total) {
        this.signal = signal;
        this.cpu = cpu;
        this.heap = heap;
        this.free = free;
        this.total = total;
    }

    @Override
    public Sample sample(long now) {
        double cpuValue = read(cpu);
        double heapValue = heapWindow.lowest(read(this::heapUsage), now);
        double memory = heapValue;
        if (signal == MemorySignal.HEAP_AND_SYSTEM) {
            double system = systemWindow.lowest(read(this::systemUsage), now);
            memory = ResourceSampler.valid(heapValue) && ResourceSampler.valid(system)
                    ? Math.max(heapValue, system)
                    : Double.NaN;
        }
        return new Sample(cpuValue, memory);
    }

    private double heapUsage() {
        var usage = heap.get();
        long max = usage.getMax() < 0 ? usage.getCommitted() : usage.getMax();
        return max > 0 ? (double) usage.getUsed() / max : Double.NaN;
    }

    private double systemUsage() {
        long size = total.getAsLong();
        long available = free.getAsLong();
        return size > 0 && available >= 0 && available <= size ? 1.0 - (double) available / size : Double.NaN;
    }

    private static double read(DoubleSupplier source) {
        try {
            double value = source.getAsDouble();
            return ResourceSampler.valid(value) ? value : Double.NaN;
        } catch (RuntimeException e) {
            return Double.NaN;
        }
    }

    private static final class Window {
        private record Reading(long at, double value) {}

        private final ArrayDeque<Reading> readings = new ArrayDeque<>();

        double lowest(double value, long now) {
            while (!readings.isEmpty() && now - readings.getFirst().at() >= TimeUnit.SECONDS.toNanos(2)) {
                readings.removeFirst();
            }
            // A stale low cannot hide a currently unavailable signal.
            if (!ResourceSampler.valid(value)) return Double.NaN;
            readings.addLast(new Reading(now, value));
            return readings.stream().mapToDouble(Reading::value).min().orElseThrow();
        }
    }
}
