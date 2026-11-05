package today.bonfire.oss.bth4j;

/**
 * The SLF4J logger names bth4j writes to. They follow the class names below {@code today.bonfire.oss.bth4j},
 * so a level set on that package covers them all, and they stay the same when an internal class moves, so a
 * log configuration or a test that captures a logger by name keeps working.
 */
public final class Loggers {
    private Loggers() {}

    /**
     * The prefix of every name below except {@link #TASKS}: a level set on it, or a log capture by it, covers all
     * the runner's own lines.
     */
    public static final String ROOT = "today.bonfire.oss.bth4j";

    /** Runner start, stop, quiet and resume, and the restart of a service thread that died. */
    public static final String RUNNER = "today.bonfire.oss.bth4j.BackgroundRunner";

    /** Schedule registration and generated ids found in use. */
    public static final String CLIENT = "today.bonfire.oss.bth4j.TaskClient";

    /** Claims, executor rejections, lease renewal and the maximum run time. */
    public static final String PROCESSOR = "today.bonfire.oss.bth4j.TaskProcessorService";

    /** One attempt of a task: failures, retries, cancellation, rescheduling, deferral and lost leases. */
    public static final String ATTEMPT = "today.bonfire.oss.bth4j.TaskExecution";

    /** Expired leases, start-by expiry, purge, worker heartbeats and stale workers. */
    public static final String MAINTENANCE = "today.bonfire.oss.bth4j.MaintenanceService";

    /** Recurring schedules: queued occurrences, skipped definitions and definitions that never fire again. */
    public static final String RECURRING = "today.bonfire.oss.bth4j.RecurringTaskService";

    /** Promotion of delayed tasks that became due. */
    public static final String PROMOTION = "today.bonfire.oss.bth4j.ScheduledTaskService";

    /** A {@link TaskObserver} that threw. */
    public static final String OBSERVERS = "today.bonfire.oss.bth4j.Observers";

    /** The default logger of {@link LoggingTaskObserver}, one line per {@link TaskEvent}. */
    public static final String TASKS = "bth4j.tasks";
}
