package today.bonfire.oss.bth4j.fault;

import java.util.Random;

/**
 * Decides which fault point of a store call fails, and how. A mechanism (a wrapped DataSource, a TCP proxy in
 * front of Redis, or a wrapped store) asks {@link #reach()} at every point where a request goes to the server
 * and does what the answer says. Points are counted over all threads, so a test that counts or cuts lets one
 * thread use the store while the plan is armed.
 */
public final class FaultPlan {

    /** Where in the request a point is cut. */
    public enum Cut {
        /** the request never reaches the server; nothing changed */
        BEFORE,
        /** the server did the work and the answer is lost */
        AFTER
    }

    private enum Mode {
        OFF,
        COUNT,
        CUT,
        RANDOM,
        DOWN
    }

    private Mode mode = Mode.OFF;
    private int reached;
    private int target;
    private Cut cut;
    private boolean fired;
    private Random random;
    private double probability;
    private int budget;
    private int faults;

    /** Counts the points reached until {@link #disarm()}, and cuts none. */
    public synchronized void count() {
        arm(Mode.COUNT);
    }

    /** Cuts the {@code point}-th point reached from now on (1 is the first), in the way {@code cut} says. */
    public synchronized void cutAt(int point, Cut cut) {
        arm(Mode.CUT);
        this.target = point;
        this.cut = cut;
    }

    /**
     * Cuts each point with the given probability, before or after with equal chance, until {@code maxFaults}
     * cuts were made. The same seed gives the same decisions for the same sequence of points.
     */
    public synchronized void failRandomly(long seed, double probability, int maxFaults) {
        arm(Mode.RANDOM);
        this.random = new Random(seed);
        this.probability = probability;
        this.budget = maxFaults;
    }

    /** Cuts every point before it reaches the server, as if the process lost its connection for good. */
    public synchronized void down() {
        arm(Mode.DOWN);
    }

    /** Stops counting and cutting and keeps the counts for {@link #reached()}, {@link #fired()} and {@link #faults()}. */
    public synchronized void disarm() {
        mode = Mode.OFF;
    }

    private void arm(Mode m) {
        mode = m;
        reached = 0;
        fired = false;
        faults = 0;
    }

    /** @return how many points were reached since the plan was armed */
    public synchronized int reached() {
        return reached;
    }

    /** @return whether {@link #cutAt} cut its point */
    public synchronized boolean fired() {
        return fired;
    }

    /** @return how many points were cut since the plan was armed, in any mode */
    public synchronized int faults() {
        return faults;
    }

    /** Called by a mechanism at every point; null lets the request through. */
    public synchronized Cut reach() {
        switch (mode) {
            case OFF:
                return null;
            case COUNT:
                reached++;
                return null;
            case CUT:
                reached++;
                if (reached != target) return null;
                fired = true;
                faults++;
                return cut;
            case RANDOM:
                reached++;
                if (faults >= budget || random.nextDouble() >= probability) return null;
                faults++;
                return random.nextBoolean() ? Cut.BEFORE : Cut.AFTER;
            default:
                reached++;
                faults++;
                return Cut.BEFORE;
        }
    }
}
