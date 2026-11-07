package today.bonfire.oss.bth4j.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import today.bonfire.oss.bth4j.Concurrency;
import today.bonfire.oss.bth4j.MemorySignal;

/** State is confined to the executor's monitor lock; time and measurements come from the caller. */
final class ResourceController {
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private final Concurrency.Adaptive settings;
    private final Signal cpu;
    private final Signal heap;
    private final Signal system;
    private final List<Signal> signals;
    private int limit;
    private Long demand;
    private boolean demandUsed;
    private Long probe;
    private double smoothedCpu = Double.NaN;
    private Long cpuHighSince;
    private Long memoryHighSince;
    private Long cpuReduction;
    private Long memoryReduction;
    private Long unknownReduction;
    private long lastActivity;
    private List<String> unavailable = List.of();

    /** @param unavailable the signals that went 5 seconds without a valid reading for the first time this tick */
    record Decision(int limit, String reason, double smoothedCpu, List<String> unavailable) {}

    ResourceController(Concurrency.Adaptive settings, long now) {
        this.settings = settings;
        limit = settings.min();
        cpu = new Signal("CPU load", now);
        heap = new Signal("heap usage", now);
        system =
                settings.memorySignal() == MemorySignal.HEAP_AND_SYSTEM ? new Signal("system memory usage", now) : null;
        signals = system == null ? List.of(cpu, heap) : List.of(cpu, heap, system);
        lastActivity = now;
    }

    void claimed(int reserved, int claimed, long now) {
        demand = reserved > 0 && claimed == reserved ? now : null;
        demandUsed = false;
        if (claimed > 0) lastActivity = now;
    }

    Decision tick(ResourceSampler.Sample sample, int occupied, int active, long now) {
        boolean validCpu = cpu.update(sample.cpu(), now);
        boolean validHeap = heap.update(sample.heap(), now);
        boolean validSystem = system == null || system.update(sample.system(), now);
        boolean validMemory = ResourceSampler.valid(sample.memory());
        if (validCpu) {
            smoothedCpu = Double.isNaN(smoothedCpu) ? sample.cpu() : smoothedCpu + 0.25 * (sample.cpu() - smoothedCpu);
        }
        cpuHighSince = since(validCpu && smoothedCpu > settings.cpuTarget() + 0.03, cpuHighSince, now);
        memoryHighSince = since(validMemory && sample.memory() >= settings.memoryTarget(), memoryHighSince, now);
        if (active > 0) lastActivity = now;
        unavailable = firstUnavailable(now);

        if (validMemory && sample.memory() >= 0.95) {
            memoryReduction = now;
            return reduce(settings.min(), "memory emergency");
        }
        if (now - lastActivity >= 30 * SECOND) return reduce(settings.min(), "idle");
        if (elapsed(memoryHighSince, now, SECOND) && ready(memoryReduction, now, SECOND)) {
            memoryReduction = now;
            return reduce(percentReduction(25), "memory target");
        }
        if (elapsed(cpuHighSince, now, SECOND) && ready(cpuReduction, now, SECOND)) {
            cpuReduction = now;
            return reduce(percentReduction(20), "CPU target");
        }
        if ((!validCpu && now - cpu.lastValid >= 5 * SECOND
                        || !validHeap && now - heap.lastValid >= 5 * SECOND
                        || !validSystem && now - system.lastValid >= 5 * SECOND)
                && ready(unknownReduction, now, SECOND)) {
            unknownReduction = now;
            return reduce(percentReduction(25), "unknown resource signal");
        }
        if (occupied >= limit
                && cpu.fresh >= 2
                && heap.fresh >= 2
                && (system == null || system.fresh >= 2)
                && ready(cpuReduction, now, 2 * SECOND)
                && ready(memoryReduction, now, 5 * SECOND)) {
            if (demand != null && now - demand < 5 * SECOND) {
                // one step per full claim: slots added by the last step must be filled before the next one
                if (!demandUsed && below(sample, 0.15)) {
                    demandUsed = true;
                    return grow(Math.min(16, Math.max(1, (limit + 3L) / 4)), "resource headroom");
                }
                if (!demandUsed && below(sample, 0.05)) {
                    demandUsed = true;
                    return grow(1, "near resource targets");
                }
            } else if (below(sample, 0.05) && ready(probe, now, 5 * SECOND)) {
                probe = now;
                return grow(1, "demand probe");
            }
        }
        return decision("hold");
    }

    private List<String> firstUnavailable(long now) {
        List<String> names = List.of();
        for (var signal : signals) {
            if (signal.firstUnavailable(now)) {
                if (names.isEmpty()) names = new ArrayList<>(signals.size());
                names.add(signal.name);
            }
        }
        return names;
    }

    private boolean below(ResourceSampler.Sample sample, double margin) {
        double cpuBand = settings.cpuTarget() - margin * Math.min(1, settings.cpuTarget() / 0.2);
        double memoryBand = settings.memoryTarget() - margin * Math.min(1, settings.memoryTarget() / 0.2);
        return sample.cpu() <= cpuBand && smoothedCpu <= cpuBand && sample.memory() <= memoryBand;
    }

    private int percentReduction(int percent) {
        return (int) Math.max(settings.min(), limit - Math.max(1, (long) limit * percent / 100));
    }

    private Decision reduce(int value, String reason) {
        limit = value;
        return decision(reason);
    }

    private Decision grow(long increment, String reason) {
        limit = (int) Math.min(settings.max(), limit + increment);
        return decision(reason);
    }

    private Decision decision(String reason) {
        return new Decision(limit, reason, smoothedCpu, unavailable);
    }

    private static Long since(boolean high, Long start, long now) {
        return high ? start == null ? now : start : null;
    }

    private static boolean elapsed(Long start, long now, long duration) {
        return start != null && now - start >= duration;
    }

    private static boolean ready(Long start, long now, long duration) {
        return start == null || elapsed(start, now, duration);
    }

    private static final class Signal {
        final String name;
        long lastValid;
        int fresh;
        boolean reported;

        Signal(String name, long now) {
            this.name = name;
            lastValid = now;
        }

        /** True once in the controller's life: at the first tick 5 seconds or more after the last valid reading. */
        boolean firstUnavailable(long now) {
            if (reported || now - lastValid < 5 * SECOND) return false;
            reported = true;
            return true;
        }

        boolean update(double value, long now) {
            if (!ResourceSampler.valid(value)) {
                fresh = 0;
                return false;
            }
            if (now - lastValid >= 5 * SECOND) fresh = 0;
            lastValid = now;
            fresh = Math.min(2, fresh + 1);
            return true;
        }
    }
}
