package today.bonfire.oss.bth4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.RedactedFailure;
import today.bonfire.oss.bth4j.spi.FailureInfo;

/** Builds every record, event payload and log text that is derived from a processor's exception. */
final class Failures {
    private static final int MAX_MESSAGE = 2 * 1024;
    private static final int MAX_STACK = 16 * 1024;

    private Failures() {}

    static FailureInfo info(FailureDetail detail, int attempt, Instant at, Throwable e) {
        if (detail == FailureDetail.CLASS_NAME_ONLY) {
            return new FailureInfo(attempt, at, e.getClass().getName(), null, null);
        }
        var w = new StringWriter();
        e.printStackTrace(new PrintWriter(w));
        return new FailureInfo(
                attempt,
                at,
                e.getClass().getName(),
                StringUtils.abbreviate(StringUtils.defaultString(e.getMessage()), MAX_MESSAGE),
                StringUtils.abbreviate(w.toString(), MAX_STACK));
    }

    /** An error the JVM cannot recover from: it is rethrown after it has been recorded. A stack overflow is not. */
    static boolean fatal(Throwable t) {
        return t instanceof VirtualMachineError && !(t instanceof StackOverflowError);
    }

    /** The throwable that goes into a {@code TaskEvent}. */
    static Throwable forEvent(FailureDetail detail, Throwable e) {
        return detail == FailureDetail.CLASS_NAME_ONLY
                ? new RedactedFailure(e.getClass().getName())
                : e;
    }

    /** The message of the exception, or its class name when the message must not be recorded. */
    static String text(FailureDetail detail, Throwable e) {
        return detail == FailureDetail.CLASS_NAME_ONLY ? e.getClass().getName() : e.getMessage();
    }

    /** {@code Throwable.toString()}, or the class name when the message must not be recorded. */
    static String describe(FailureDetail detail, Throwable e) {
        return detail == FailureDetail.CLASS_NAME_ONLY ? e.getClass().getName() : e.toString();
    }
}
