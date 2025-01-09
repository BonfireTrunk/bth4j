package today.bonfire.oss.bth4j.common;

import today.bonfire.oss.bth4j.Task;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Task handler constants
 */
public class THC {
  public static final Consumer<Task>              EMPTY_CONSUMER   = (t) -> {};
  public static final BiConsumer<Task, Exception> EMPTY_BICONSUMER = (t, u) -> {};

  public static class Keys {
    public static final String LOCK_SUFFIX                = ":LOCK";
    public static final String SCHEDULED_TASK_QUEUE       = "BTH:SCHEDULED_TASKS";
    public static final String RECURRING_TASK_SET         = "BTH:CRON_TASKS";
    public static final String DEAD_TASKS                 = "BTH:DEAD_TASKS";
    public static final String TEMP_ROTATION_LIST         = "BTH:TEMP_LIST";
    public static final String IN_PROGRESS_TASKS          = "BTH:IN_PROGRESS";
    public static final String TASK_RETRY_COUNT           = "BTH:TASK_RETRY_COUNT";
    public static final String LOCK_SCHEDULED_TASKS_QUEUE = SCHEDULED_TASK_QUEUE + LOCK_SUFFIX;
    public static final String LOCK_ROTATION_LIST         = TEMP_ROTATION_LIST + LOCK_SUFFIX;
    public static final String LOCK_IN_PROGRESS_TASKS     = IN_PROGRESS_TASKS + LOCK_SUFFIX;
    public static final String LOCK_RECURRING_TASKS       = RECURRING_TASK_SET + LOCK_SUFFIX;

  }

  public static class Prefixes {
    public static final String DATA = "BTH:D:";
  }

  /**
   * time is always in seconds
   */
  public static class Time {
    public static final long T_30_DAYS    = 3600L * 24 * 30;
    public static final long T_2_DAYS     = 3600L * 24 * 2;
    public static final long T_1_DAY      = 3600L * 24;
    public static final long T_1_HOUR     = 3600L;
    public static final long T_30_MINUTES = 1800L;
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
