package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/** How many tasks a runner executes at once. */
public sealed interface Concurrency {

    /** A fixed number of tasks run at once. */
    record Fixed(int limit) implements Concurrency {
        public Fixed {
            if (limit < 1 || limit > 100_000) throw new TaskConfigurationException("concurrency must be 1 to 100000");
        }
    }

    /** A resource target controller bounded by {@code min} and {@code max}, starting at {@code min}. */
    record Adaptive(int min, int max, double cpuTarget, double memoryTarget, MemorySignal memorySignal)
            implements Concurrency {
        public Adaptive {
            if (min < 1 || max < min)
                throw new TaskConfigurationException("adaptive concurrency needs 1 <= min <= max");
            if (!Double.isFinite(cpuTarget) || cpuTarget <= 0 || cpuTarget > 0.95)
                throw new TaskConfigurationException("adaptive cpuTarget must be finite and 0 < cpuTarget <= 0.95");
            if (!Double.isFinite(memoryTarget) || memoryTarget <= 0 || memoryTarget > 0.90)
                throw new TaskConfigurationException(
                        "adaptive memoryTarget must be finite and 0 < memoryTarget <= 0.90");
            if (memorySignal == null) throw new TaskConfigurationException("adaptive memorySignal must not be null");
        }

        public Adaptive(int min, int max) {
            this(min, max, 0.90, 0.80, MemorySignal.HEAP);
        }

        public Adaptive withMemorySignal(MemorySignal signal) {
            return new Adaptive(min, max, cpuTarget, memoryTarget, signal);
        }
    }

    static Concurrency fixed(int limit) {
        return new Fixed(limit);
    }

    /** Adaptive limits with CPU target 0.90, memory target 0.80 and heap memory pressure. */
    static Adaptive adaptive(int min, int max) {
        return new Adaptive(min, max);
    }

    /** Adaptive limits with explicit CPU and memory targets and heap memory pressure. */
    static Adaptive adaptive(int min, int max, double cpuTarget, double memoryTarget) {
        return new Adaptive(min, max, cpuTarget, memoryTarget, MemorySignal.HEAP);
    }

    /** Twice the number of available processors. */
    static Concurrency defaultConcurrency() {
        return new Fixed(Math.max(2, Runtime.getRuntime().availableProcessors() * 2));
    }
}
