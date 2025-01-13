package today.bonfire.oss.bth4j.common;

import today.bonfire.oss.bth4j.service.Task;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Task handler constants
 */
public class THC {
  public static final Consumer<Task>              EMPTY_CONSUMER   = (t) -> {};
  public static final BiConsumer<Task, Exception> EMPTY_BICONSUMER = (t, u) -> {};

  public static class Keys {
    public final String NAMESPACE;
    public final String DATA;
    public final String SCHEDULED_TASK_QUEUE;
    public final String RECURRING_TASK_SET;
    public final String DEAD_TASKS;
    public final String TEMP_ROTATION_LIST;
    public final String IN_PROGRESS_TASKS;
    public final String TASK_RETRY_COUNT;
    public final String LOCK_SCHEDULED_TASKS_QUEUE;
    public final String LOCK_ROTATION_LIST;
    public final String LOCK_IN_PROGRESS_TASKS;
    public final String LOCK_RECURRING_TASKS;

    public Keys(String namespace) {
      this.NAMESPACE = namespace;
      String namespacePrefix = namespace + ":";
      this.DATA = namespacePrefix + "D:";
      final var scheduledTasks = "SCHEDULED_TASKS";
      final var cronTasks      = "CRON_TASKS";
      final var deadTasks      = "DEAD_TASKS";
      final var tempList       = "TEMP_LIST";
      final var inProgress     = "IN_PROGRESS";
      final var taskRetryCount = "TASK_RETRY_COUNT";
      this.SCHEDULED_TASK_QUEUE = namespacePrefix + scheduledTasks;
      this.RECURRING_TASK_SET   = namespacePrefix + cronTasks;
      this.DEAD_TASKS           = namespacePrefix + deadTasks;
      this.TEMP_ROTATION_LIST   = namespacePrefix + tempList;
      this.IN_PROGRESS_TASKS    = namespacePrefix + inProgress;

      this.TASK_RETRY_COUNT = namespacePrefix + taskRetryCount;
      String LOCK_PREFIX = namespacePrefix + "LOCK:";

      this.LOCK_SCHEDULED_TASKS_QUEUE = LOCK_PREFIX + scheduledTasks;
      this.LOCK_ROTATION_LIST         = LOCK_PREFIX + tempList;
      this.LOCK_IN_PROGRESS_TASKS     = LOCK_PREFIX + inProgress;
      this.LOCK_RECURRING_TASKS       = LOCK_PREFIX + cronTasks;
    }
  }

  /**
   * time is always in seconds
   */
  public static class Time {
    public static final long T_30_DAYS    = 3600L * 24 * 30;
    public static final long T_1_HOUR     = 3600L;
    public static final long T_10_MINUTES = 600L;
    public static final long T_5_MINUTES  = 300L;
    public static final long T_2_MINUTES  = 120;
    public static final long T_1_MINUTE   = 60L;
    public static final long T_30_SECONDS = 30L;
    public static final long T_10_SECONDS = 10L;
    public static final long T_5_SECONDS  = 5L;
    public static final long T_3_SECONDS  = 3L;
    public static final long T_1_SECOND   = 1L;

  }
}
