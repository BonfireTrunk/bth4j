package today.bonfire.oss.bth4j;

import java.util.function.LongSupplier;
import org.slf4j.Logger;

/**
 * Logs the failed rounds of one loop while its store does not answer, so that an outage leaves a few lines and not
 * a stack trace per round. A loop cannot tell that two failures belong to one outage, only that none of its store
 * calls succeeded in between, and that is what this class goes by.
 *
 * <p>
 * The first failure after a success is logged as the loop logged it before, with its stack trace. A later failure
 * of the same kind is one line without a stack trace, and a failure of another kind has its stack trace again. The
 * kind is the exception class plus the class of its root cause; messages are not compared, as they carry
 * addresses and counters. The first success after failures logs one INFO line with the number of failed rounds and
 * the time they took.
 *
 * <p>
 * Safe for several threads. The success path reads one volatile field and does nothing else when no round failed.
 */
final class OutageLog {
    private static final int MAX_CAUSES = 64;

    private final Logger log;
    private final String loop;
    private final LongSupplier nanoTime;

    /** the failed rounds in a row; written under the lock, read without it by {@link #succeeded()} */
    private volatile long failures;

    private Class<?> kind;
    private Class<?> rootKind;
    private long firstFailureNanos;

    /**
     * @param loop names the loop in the recovery line, for example "Task processor"
     */
    OutageLog(Logger log, String loop, LongSupplier nanoTime) {
        this.log = log;
        this.loop = loop;
        this.nanoTime = nanoTime;
    }

    /**
     * Logs a failed round at ERROR. {@code message} and {@code args} are the loop's own line, as it is logged with
     * a stack trace; a repeat of the same kind appends the number of failures in a row and the exception's class
     * and message, and drops the stack trace.
     */
    void failed(Throwable e, String message, Object... args) {
        long count;
        boolean repeat;
        synchronized (this) {
            var root = rootOf(e);
            count = failures + 1;
            repeat = count > 1 && kind == e.getClass() && rootKind == root;
            if (count == 1) firstFailureNanos = nanoTime.getAsLong();
            kind = e.getClass();
            rootKind = root;
            failures = count;
        }
        if (repeat) {
            var line = new Object[args.length + 3];
            System.arraycopy(args, 0, line, 0, args.length);
            line[args.length] = count;
            line[args.length + 1] = e.getClass().getName();
            line[args.length + 2] = e.getMessage();
            log.error(message + " (failure {} in a row: {}: {})", line);
        } else {
            var line = new Object[args.length + 1];
            System.arraycopy(args, 0, line, 0, args.length);
            line[args.length] = e;
            log.error(message, line);
        }
    }

    /** Call where a store call of the loop returned normally. */
    void succeeded() {
        if (failures == 0) return;
        long count;
        long tenths;
        synchronized (this) {
            count = failures;
            if (count == 0) return;
            tenths = (nanoTime.getAsLong() - firstFailureNanos) / 100_000_000L;
            failures = 0;
            kind = null;
            rootKind = null;
        }
        log.info(
                "{}: the store answers again after {} failed attempts over {} seconds",
                loop,
                count,
                tenths / 10 + "." + tenths % 10);
    }

    /** The class of the last cause in the chain; the walk is capped, so a chain that loops back on itself ends. */
    private static Class<?> rootOf(Throwable e) {
        var t = e;
        for (int i = 0; i < MAX_CAUSES; i++) {
            var cause = t.getCause();
            if (cause == null) break;
            t = cause;
        }
        return t.getClass();
    }
}
