package today.bonfire.oss.bth4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Logs one line per event through SLF4J with the task id, type and queue in the MDC
 * ({@code bth4j.task}, {@code bth4j.event}, {@code bth4j.queue}). Succeeded, Started and Purged log at
 * debug, retries and cancellations at info, lost leases and deaths at warn.
 */
public final class LoggingTaskObserver implements TaskObserver {
    private final Logger log;

    public LoggingTaskObserver() {
        this(LoggerFactory.getLogger("bth4j.tasks"));
    }

    public LoggingTaskObserver(Logger log) {
        this.log = log;
    }

    @Override
    public void onEvent(TaskEvent event) {
        switch (event) {
            case TaskEvent.Started e -> withTask(e.task(), () -> log.debug("started on worker {}", e.workerId()));
            case TaskEvent.Succeeded e ->
                withTask(e.task(), () -> log.debug("succeeded in {} ({})", e.runTime(), e.outcome()));
            case TaskEvent.Retrying e ->
                withTask(
                        e.task(),
                        () -> log.info(
                                "attempt {} failed, retry at {} ({}): {}",
                                e.task().attempts(),
                                e.retryAt(),
                                e.outcome(),
                                e.error().toString()));
            case TaskEvent.Died e ->
                withTask(
                        e.task(),
                        () -> log.warn(
                                "died after {} attempts: {} ({})", e.task().attempts(), e.reason(), e.outcome()));
            case TaskEvent.Rescheduled e ->
                withTask(e.task(), () -> log.info("rescheduled to {} ({})", e.due(), e.outcome()));
            case TaskEvent.Cancelled e ->
                withTask(e.task(), () -> log.info("cancelled: {} ({})", e.reason(), e.outcome()));
            case TaskEvent.LeaseExpired e -> {
                MDC.put("bth4j.task", Long.toString(e.id()));
                try {
                    log.warn(
                            "lease expired after attempt {} of {}, {}",
                            e.attempts(),
                            e.maxAttempts(),
                            e.dead() ? "dead" : "retried");
                } finally {
                    MDC.remove("bth4j.task");
                }
            }
            case TaskEvent.Deferred e -> log.warn("task {} ({}) deferred: {}", e.id(), e.event(), e.reason());
            case TaskEvent.Expired e -> log.info("task {} expired before it could start", e.id());
            case TaskEvent.ScheduleFired e ->
                log.info("schedule {} fired for {} as task {}", e.scheduleId(), e.fireAt(), e.taskId());
            case TaskEvent.Purged e -> log.debug("purged {} {} tasks", e.count(), e.state());
        }
    }

    private void withTask(TaskView task, Runnable body) {
        MDC.put("bth4j.task", Long.toString(task.id()));
        MDC.put("bth4j.event", task.event());
        MDC.put("bth4j.queue", task.queue());
        try {
            body.run();
        } finally {
            MDC.remove("bth4j.task");
            MDC.remove("bth4j.event");
            MDC.remove("bth4j.queue");
        }
    }
}
