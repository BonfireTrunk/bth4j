package today.bonfire.oss.bth4j;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/** Collects what the named loggers write while open, one summary line per event. */
final class LogCapture implements AutoCloseable {
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final List<Logger> loggers;

    LogCapture(String... names) {
        loggers = java.util.Arrays.stream(names)
                .map(n -> (Logger) LoggerFactory.getLogger(n))
                .toList();
        appender.start();
        loggers.forEach(l -> l.addAppender(appender));
    }

    /** The level, the formatted message and "[trace]" when the event carries a throwable, for each event. */
    List<String> lines() {
        return appender.list.stream()
                .map(e -> e.getLevel() + " " + e.getFormattedMessage()
                        + (e.getThrowableProxy() != null ? " [trace]" : ""))
                .toList();
    }

    @Override
    public void close() {
        loggers.forEach(l -> l.detachAppender(appender));
    }
}
