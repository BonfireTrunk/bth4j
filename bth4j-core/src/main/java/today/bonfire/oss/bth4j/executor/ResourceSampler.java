package today.bonfire.oss.bth4j.executor;

@FunctionalInterface
interface ResourceSampler {
    record Sample(double cpu, double memory) {}

    Sample sample(long nowNanos);

    static boolean valid(double value) {
        return Double.isFinite(value) && value >= 0 && value <= 1;
    }
}
