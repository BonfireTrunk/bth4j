package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.Task;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.exceptions.OperationNotAllowed;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class TaskOps {

  private static JedisPooled jedis;
  private static JsonMapper  mapper;

  static Task getTaskFromQueue(String queueName) {
    var t = jedis.lmove(queueName, THC.Keys.TEMP_ROTATION_LIST, ListDirection.RIGHT, ListDirection.LEFT);
    if (t != null) {
      try (var transaction = jedis.multi()) {
        transaction.zadd(THC.Keys.IN_PROGRESS_TASKS, ((double) Instant.now().getEpochSecond()), t);
        transaction.lrem(THC.Keys.TEMP_ROTATION_LIST, 1, t);
        transaction.exec();
      }
    }
    return new Task(t);
  }

  public static void init(JedisPooled jedisService, JsonMapper jsonMapper) {
    if (jedis != null) {
      throw new OperationNotAllowed("Jedis is already initialized");
    }
    jedis  = jedisService;
    mapper = jsonMapper;
  }

  public static void addTaskToQueue(Task task, Object data) {
    addTaskToQueue(task, data, false);
  }

  /**
   *
   */
  static void addTaskToQueue(Task task, Object data, boolean isDataRaw) {
    String queueName = ObjectUtils.firstNonNull(task.queueName(), BgrParent.DEFAULT_QUEUE);
    try (var transaction = jedis.multi()) {
      if (ObjectUtils.isNotEmpty(data)) {
        transaction.set(THC.Prefixes.DATA + task.uniqueId(), isDataRaw ? data.toString() : mapper.toJson(data));
      }

      if (task.isExecutionTimeSet()) {
        transaction.zadd(THC.Keys.SCHEDULED_TASK_QUEUE, ((double) task.executeAtTimeStamp().getEpochSecond()),
                         task.taskString());
      } else {
        transaction.lpush(queueName, task.taskString());
      }
      transaction.exec();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    log.info("Added {} to queue: {}", task, queueName);
  }

  public static String addRecurringTask(Task task) {
    jedis.hset(THC.Keys.RECURRING_TASK_SET, task.taskString(), Instant.now().getEpochSecond() + "");
    log.info("Added recurring task: {}", task);
    return task.taskString();
  }

  public static void deleteRecurringTask(Task task) {
    jedis.hdel(THC.Keys.RECURRING_TASK_SET, task.taskString());
    log.info("Deleted recurring task: {}", task);
  }

  public static String getDataForTask(String uniqueId) {
    if (StringUtils.isBlank(uniqueId)) return null;
    return jedis.get(THC.Prefixes.DATA + uniqueId);
  }

  public static <T> T getDataForTask(String uniqueId, Class<T> clazz) {
    if (StringUtils.isBlank(uniqueId) || clazz == null) {
      return null;
    }
    try {
      var r = jedis.get(THC.Prefixes.DATA + uniqueId);
      return mapper.fromJson(r, clazz);
    } catch (Exception e) {
      log.error("Error getting data for task: {}", uniqueId, e);
      throw new TaskDataException("Error getting data for task: " + uniqueId);
    }
  }

  static int incrementRetryCount(String uniqueId) {
    return (int) jedis.hincrBy(THC.Keys.TASK_RETRY_COUNT, uniqueId, 1);
  }

  public static void updateExecutionTimeForRecurringTasks(String key, Instant instant) {
    jedis.hset(THC.Keys.RECURRING_TASK_SET, key, String.valueOf(instant.getEpochSecond()));
  }

  static String getLock(String lockName) {
    return jedis.get(lockName);
  }

  static boolean acquireLock(String lockKey, long expiryTime) {
    var r = jedis.set(lockKey, String.valueOf(Instant.now().getEpochSecond()),
                      new SetParams().nx().ex(expiryTime));
    return r != null;
  }

  static boolean refreshLock(String lockKey, long expiryTime) {
    return BooleanUtils.toBoolean((int) jedis.expire(lockKey, expiryTime));
  }

  static void releaseLock(String lockDelayedTasks) {
    jedis.del(lockDelayedTasks);
  }

  public static long queueSize(String queueName) {
    return jedis.llen(queueName);
  }

  static void deleteTaskFromInProgressQueue(Task task) {
    try (var pipeline = jedis.pipelined()) {
      pipeline.del(THC.Prefixes.DATA + task.uniqueId());
      pipeline.zrem(THC.Keys.IN_PROGRESS_TASKS, task.taskString());
      pipeline.hdel(THC.Keys.TASK_RETRY_COUNT, task.uniqueId());
    }
  }

  static Map<String, Double> peek(String delayedTaskQueue, int numberOfItems) {
    var r   = jedis.zrangeWithScores(delayedTaskQueue, 0, numberOfItems - 1);
    var map = new HashMap<String, Double>();
    r.forEach(e -> map.put(e.getElement(), e.getScore()));
    return map;
  }

  static void addItemsFromDelayedTasksToQueue(List<String> tasks) {
    try (var transaction = jedis.multi()) {
      tasks.forEach(t -> {
        var task = new Task(t);
        if (task.queueName() != null) {
          transaction.rpush(task.queueName(), task.taskString());
        } else {
          transaction.rpush(BgrParent.DEFAULT_QUEUE, task.taskString());
        }
      });
      transaction.zrem(THC.Keys.SCHEDULED_TASK_QUEUE, tasks.toArray(new String[0]));
      transaction.exec();
    }
  }

  static List<String> getAllItemsFromList(String listName) {
    return jedis.lrange(listName, 0, -1);
  }

  static void deleteFromRotationListAndAddToQueue(List<String> items) {
    try (var transaction = jedis.multi()) {
      items.forEach(t -> {
        var task = new Task(t);
        if (task.queueName() != null) {
          transaction.rpush(task.queueName(), task.taskString());
        } else {
          transaction.rpush(BgrParent.DEFAULT_QUEUE, task.taskString());
        }
        transaction.lrem(THC.Keys.TEMP_ROTATION_LIST, 1, task.taskString());
      });
      transaction.exec();
    }
  }

  public static long getNumberOfTaskInProgress() {
    return jedis.zcard(THC.Keys.IN_PROGRESS_TASKS);
  }

  static ScanResult<Tuple> scanSortedSet(String key, String cursor) {
    return jedis.zscan(key, cursor);
  }

  static ScanResult<Map.Entry<String, String>> scanHashSet(String key, String cursor) {
    return jedis.hscan(key, cursor, new ScanParams().count(200));
  }

  static void deleteFromInProgressQueueAndAddToQueue(
      List<Task> tasksToRetry, List<Task> tasksToDelete) {
    if (tasksToRetry.isEmpty() && tasksToDelete.isEmpty()) return;

    if (!tasksToRetry.isEmpty()) {
      try (var transaction = jedis.multi()) {
        tasksToRetry.forEach(t -> {
          transaction.zadd(THC.Keys.SCHEDULED_TASK_QUEUE, t.executeAtTimeStamp().getEpochSecond(),
                           t.taskString());
          transaction.zrem(THC.Keys.IN_PROGRESS_TASKS, t.taskString());
        });
        transaction.exec();
      }
    }

    if (tasksToDelete.isEmpty()) return;
    try (var transaction = jedis.multi()) {
      tasksToDelete.forEach(t -> {
        transaction.zadd(THC.Keys.DEAD_TASKS, Instant.now().getEpochSecond(), t.taskString());
        transaction.zrem(THC.Keys.IN_PROGRESS_TASKS, t.taskString());
        transaction.expire(THC.Prefixes.DATA + t.uniqueId(), THC.Time.T_30_DAYS);
        transaction.hdel(THC.Keys.TASK_RETRY_COUNT, t.uniqueId());
      });
    }
  }

  static void moveToDeadQueue(Task task) {
    try (var transaction = jedis.multi()) {
      transaction.zadd(THC.Keys.DEAD_TASKS, Instant.now().getEpochSecond(), task.taskString());
      transaction.expire(THC.Prefixes.DATA + task.uniqueId(), THC.Time.T_30_DAYS);
      transaction.zrem(THC.Keys.IN_PROGRESS_TASKS, task.taskString());
      transaction.hdel(THC.Keys.TASK_RETRY_COUNT, task.uniqueId());
      transaction.exec();
    }
  }

  static void removeTasksFromSortedSetBasedOnTime(String key, Instant time) {
    if (!StringUtils.equalsAny(key, THC.Keys.DEAD_TASKS)) {
      throw new OperationNotAllowed(
          "This method is only allowed to delete tasks from the dead queue");
    }

    Supplier<List<String>> tasksSupplier =
        () -> jedis.zrangeByScore(key, 0, time.getEpochSecond(), 0, 100);
    do {
      var tasks = tasksSupplier.get();
      if (ObjectUtils.isEmpty(tasks)) break;
      tasks.forEach(t -> {
        var task = new Task(t);
        try (var transaction = jedis.multi()) {
          transaction.zrem(key, task.taskString());
          transaction.del(THC.Prefixes.DATA + task.uniqueId());
          transaction.exec();
        }
      });
    } while (true);
  }

  static boolean isRetryCountExhausted(Event event, int timeRan) {
    return timeRan > event.retryCount();
  }
}
