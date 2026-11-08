package today.bonfire.oss.bth4j.executor;

@FunctionalInterface
interface ResourceSampler {
    record Sample(double cpu, double heap, double system) {
        Sample(double cpu, double memory) {
            this(cpu, memory, Double.NaN);
        }

        double memory() {
            if (!valid(heap)) return valid(system) ? system : Double.NaN;
            return valid(system) ? Math.max(heap, system) : heap;
        }
    }

    Sample sample(long nowNanos);

    static boolean valid(double value) {
        return Double.isFinite(value) && value >= 0 && value <= 1;
    }
}
