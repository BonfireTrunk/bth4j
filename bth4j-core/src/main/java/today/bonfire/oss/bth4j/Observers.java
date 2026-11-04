package today.bonfire.oss.bth4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delivers events to the configured observer and keeps its failures away from the task outcome. */
final class Observers {
    private static final Logger log = LoggerFactory.getLogger(Observers.class);

    private final TaskObserver observer;

    Observers(TaskObserver observer) {
        this.observer = observer == null ? TaskObserver.none() : observer;
    }

    void emit(TaskEvent event) {
        emitReporting(event);
    }

    /** Delivers the event like {@link #emit}; returns the fatal error the observer threw, if any, after logging it. */
    Error emitReporting(TaskEvent event) {
        try {
            observer.onEvent(event);
        } catch (RuntimeException | Error e) {
            log.error("Task observer failed on {}", event.getClass().getSimpleName(), e);
            if (e instanceof Error err && Failures.fatal(err)) return err;
        }
        return null;
    }
}
