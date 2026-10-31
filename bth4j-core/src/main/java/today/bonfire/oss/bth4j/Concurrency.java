package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

/** How many tasks a runner executes at once. */
public sealed interface Concurrency {

    /** A fixed number of tasks run at once. */
    record Fixed(int limit) implements Concurrency {
        public Fixed {
            if (limit < 1 || limit > 100_000) throw new TaskConfigurationError("concurrency must be 1 to 100000");
        }
    }

    /**
     * The limit starts at the number of processors and moves between {@code min} and {@code max} with CPU load
     * and throughput, measured once per second.
     */
    record Adaptive(int min, int max) implements Concurrency {
        public Adaptive {
            if (min < 1 || max < min) throw new TaskConfigurationError("adaptive concurrency needs 1 <= min <= max");
        }
    }

    static Concurrency fixed(int limit) {
        return new Fixed(limit);
    }

    static Concurrency adaptive(int min, int max) {
        return new Adaptive(min, max);
    }

    /** Twice the number of available processors. */
    static Concurrency defaultConcurrency() {
        return new Fixed(Math.max(2, Runtime.getRuntime().availableProcessors() * 2));
    }
}
