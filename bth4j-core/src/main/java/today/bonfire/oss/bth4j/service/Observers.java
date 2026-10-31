package today.bonfire.oss.bth4j.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.TaskObserver;

/** Delivers events to the configured observer and keeps its failures away from the task outcome. */
public final class Observers {
    private static final Logger log = LoggerFactory.getLogger(Observers.class);

    private final TaskObserver observer;

    public Observers(TaskObserver observer) {
        this.observer = observer == null ? TaskObserver.none() : observer;
    }

    public void emit(TaskEvent event) {
        try {
            observer.onEvent(event);
        } catch (RuntimeException e) {
            log.error("Task observer failed on {}", event.getClass().getSimpleName(), e);
        }
    }
}
