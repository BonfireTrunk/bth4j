package today.bonfire.oss.bth4j;

/**
 * Receives every {@link TaskEvent} of a runner. Called on the thread that produced the event; exceptions are
 * logged and never change a task's outcome. Keep it fast or hand the event to another thread.
 */
@FunctionalInterface
public interface TaskObserver {

    void onEvent(TaskEvent event);

    static TaskObserver none() {
        return event -> {};
    }
}
