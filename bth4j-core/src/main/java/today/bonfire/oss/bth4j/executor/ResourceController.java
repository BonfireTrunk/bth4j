package today.bonfire.oss.bth4j.executor;

import java.util.concurrent.TimeUnit;
import today.bonfire.oss.bth4j.Concurrency;

/** State is confined to the executor's monitor lock; time and measurements come from the caller. */
final class ResourceController {
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private final Concurrency.Adaptive settings;
    private final Signal cpu;
    private final Signal memory;
    private int limit;
    private boolean credit;
    private double smoothedCpu = Double.NaN;
    private Long cpuHighSince;
    private Long memoryHighSince;
    private Long cpuReduction;
    private Long memoryReduction;
    private Long unknownReduction;
    private long lastOccupied;

    record Decision(int limit, String reason, double smoothedCpu) {}

    ResourceController(Concurrency.Adaptive settings, long now) {
        this.settings = settings;
        limit = settings.min();
        cpu = new Signal(now);
        memory = new Signal(now);
        lastOccupied = now;
    }

    void claimed(int reserved, int claimed) {
        credit = reserved > 0 && claimed == reserved;
    }

    void occupied(long now) {
        lastOccupied = Math.max(lastOccupied, now);
    }

    Decision tick(ResourceSampler.Sample sample, int occupied, long now) {
        boolean validCpu = cpu.update(sample.cpu(), now);
        boolean validMemory = memory.update(sample.memory(), now);
        if (validCpu) {
            smoothedCpu = Double.isNaN(smoothedCpu) ? sample.cpu() : smoothedCpu + 0.25 * (sample.cpu() - smoothedCpu);
        }
        cpuHighSince = since(validCpu && smoothedCpu > settings.cpuTarget() + 0.03, cpuHighSince, now);
        memoryHighSince = since(validMemory && sample.memory() >= settings.memoryTarget(), memoryHighSince, now);
        if (occupied > 0) occupied(now);

        if (validMemory && sample.memory() >= 0.95) {
            memoryReduction = now;
            return reduce(settings.min(), "memory emergency");
        }
        if (now - lastOccupied >= 30 * SECOND) return reduce(settings.min(), "idle");
        if (elapsed(memoryHighSince, now, SECOND) && ready(memoryReduction, now, SECOND)) {
            memoryReduction = now;
            return reduce(percentReduction(25), "memory target");
        }
        if (elapsed(cpuHighSince, now, SECOND) && ready(cpuReduction, now, SECOND)) {
            cpuReduction = now;
            return reduce(percentReduction(20), "CPU target");
        }
        if ((!validCpu && now - cpu.lastValid >= 5 * SECOND || !validMemory && now - memory.lastValid >= 5 * SECOND)
                && ready(unknownReduction, now, SECOND)) {
            unknownReduction = now;
            return reduce(percentReduction(25), "unknown resource signal");
        }
        if (credit
                && occupied <= limit
                && cpu.fresh >= 2
                && memory.fresh >= 2
                && ready(cpuReduction, now, 2 * SECOND)
                && ready(memoryReduction, now, 5 * SECOND)) {
            if (below(sample, 0.15)) {
                return grow(Math.min(16, Math.max(1, (limit + 3L) / 4)), "resource headroom");
            }
            if (below(sample, 0.05)) return grow(1, "near resource targets");
        }
        return decision("hold");
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
        credit = false;
        limit = value;
        return decision(reason);
    }

    private Decision grow(long increment, String reason) {
        credit = false;
        limit = (int) Math.min(settings.max(), limit + increment);
        return decision(reason);
    }

    private Decision decision(String reason) {
        return new Decision(limit, reason, smoothedCpu);
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
        long lastValid;
        int fresh;

        Signal(long now) {
            lastValid = now;
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
