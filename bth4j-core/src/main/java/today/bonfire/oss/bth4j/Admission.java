package today.bonfire.oss.bth4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What decides whether a runner may start tasks and whether it is idle, shared by every processor generation of
 * a runner: the quiet flag, the claims in progress and the attempts that have not finished. A replacement
 * processor reads the same state as the one it replaces, so {@code quiet()} and {@code resume()} cannot miss it.
 * <p>
 * A claim is entered before it checks the quiet flag, and {@link #quiet(boolean)} is written before
 * {@link #idle()} reads the claim count; both are volatile, so a claim either sees the runner quiet and backs
 * off, or {@link #idle()} sees the claim. A claim is left only after every task it returned is registered as
 * an attempt, so there is no instant at which a task is neither claimed nor running.
 * <p>
 * Attempts are tracked by identity: the retry of a task whose earlier attempt is still running on this runner
 * is a second attempt, not a replacement of the first.
 */
final class Admission {
    private volatile boolean quiet;
    private final AtomicInteger claims = new AtomicInteger();
    private final Set<TaskExecution> attempts = ConcurrentHashMap.newKeySet();

    boolean quiet() {
        return quiet;
    }

    void quiet(boolean quiet) {
        this.quiet = quiet;
    }

    /** @return true when the claim may go ahead; it must then be ended with {@link #exitClaim()} */
    boolean enterClaim() {
        claims.incrementAndGet();
        if (!quiet) return true;
        claims.decrementAndGet();
        return false;
    }

    void exitClaim() {
        claims.decrementAndGet();
    }

    void started(TaskExecution attempt) {
        attempts.add(attempt);
    }

    void finished(TaskExecution attempt) {
        attempts.remove(attempt);
    }

    /** @return a snapshot of the attempts that have not finished, including those writing their outcome */
    List<TaskExecution> attempts() {
        return List.copyOf(attempts);
    }

    int running() {
        return attempts.size();
    }

    /** @return true when no claim is in progress and no attempt is unfinished */
    boolean idle() {
        return claims.get() == 0 && attempts.isEmpty();
    }
}
