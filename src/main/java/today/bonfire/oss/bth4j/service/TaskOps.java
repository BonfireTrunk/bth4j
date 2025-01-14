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
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.exceptions.OperationNotAllowed;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class TaskOps {

  private final JedisPooled             jedis;
  private final JsonMapper              mapper;
  private final THC.Keys                keys;
  private final Function<String, Event> eventParser;
  private final QueuesHolder            queuesHolder;

  public TaskOps(THC.Keys keys, JedisPooled jedis, JsonMapper jsonMapper,
                 Function<String, Event> eventParser, QueuesHolder queuesHolder) {
    this.jedis        = jedis;
    this.mapper       = jsonMapper;
    this.keys         = keys;
    this.eventParser  = eventParser;
    this.queuesHolder = queuesHolder;
  }

  boolean isRetryCountExhausted(Event event, int timeRan) {
    return timeRan > event.retryCount();
  }

  Task getTaskFromQueue(String queueName) {
    var t = jedis.lmove(queueName, keys.TEMP_ROTATION_LIST, ListDirection.RIGHT, ListDirection.LEFT);
    if (t != null) {
      try (var transaction = jedis.multi()) {
        transaction.zadd(keys.IN_PROGRESS_TASKS, ((double) Instant.now().getEpochSecond()), t);
        transaction.lrem(keys.TEMP_ROTATION_LIST, 1, t);
        transaction.exec();
      }
    }
    return new Task(t, eventParser);
  }

  public void addTaskToQueue(Task task, Object data) {
    addTaskToQueue(task, data, false);
  }

  /**
   *
   */
  void addTaskToQueue(Task task, Object data, boolean isDataRaw) {
    var queueName = task.queueName();
    if (StringUtils.isBlank(queueName)) {
      queueName = queuesHolder.defaultQueue;
    } else {
      if (!queuesHolder.queueNames.contains(queueName)) {
        throw new TaskErrorException("Queue name is not in available list: " + queueName);
      }
    }
    ObjectUtils.firstNonNull(data, task.data()); // may remove use with caution
    try (var transaction = jedis.multi()) {
      if (ObjectUtils.isNotEmpty(data)) {
        transaction.set(keys.DATA + task.uniqueId(), isDataRaw ? data.toString() : mapper.toJson(data));
      }

      if (task.isExecutionTimeSet()) {
        transaction.zadd(keys.SCHEDULED_TASK_QUEUE, ((double) task.executeAtTimeStamp().getEpochSecond()),
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

  public String addRecurringTask(Task task) {
    jedis.hset(keys.RECURRING_TASK_SET, task.taskString(), Instant.now().getEpochSecond() + "");
    log.info("Added recurring task: {}", task);
    return task.taskString();
  }

  public void deleteRecurringTask(Task task) {
    jedis.hdel(keys.RECURRING_TASK_SET, task.taskString());
    log.info("Deleted recurring task: {}", task);
  }

  public String getDataForTask(String uniqueId) {
    if (StringUtils.isBlank(uniqueId)) return null;
    return jedis.get(keys.DATA + uniqueId);
  }

  public <T> T getDataForTask(String uniqueId, Class<T> clazz) {
    if (StringUtils.isBlank(uniqueId) || clazz == null) {
      return null;
    }
    try {
      var r = jedis.get(keys.DATA + uniqueId);
      return mapper.fromJson(r, clazz);
    } catch (Exception e) {
      log.error("Error getting data for task: {}", uniqueId, e);
      throw new TaskDataException("Error getting data for task: " + uniqueId);
    }
  }

  int incrementRetryCount(String uniqueId) {
    return (int) jedis.hincrBy(keys.TASK_RETRY_COUNT, uniqueId, 1);
  }

  public void updateExecutionTimeForRecurringTasks(String key, Instant instant) {
    jedis.hset(keys.RECURRING_TASK_SET, key, String.valueOf(instant.getEpochSecond()));
  }

  String getLock(String lockName) {
    return jedis.get(lockName);
  }

  boolean acquireLock(String lockKey, long expiryTime) {
    var r = jedis.set(lockKey, String.valueOf(Instant.now().getEpochSecond()),
                      new SetParams().nx().ex(expiryTime));
    return r != null;
  }

  boolean refreshLock(String lockKey, long expiryTime) {
    return BooleanUtils.toBoolean((int) jedis.expire(lockKey, expiryTime));
  }

  void releaseLock(String lockDelayedTasks) {
    jedis.del(lockDelayedTasks);
  }

  public long queueSize(String queueName) {
    return jedis.llen(queueName);
  }

  void deleteTaskFromInProgressQueue(Task task) {
    try (var pipeline = jedis.pipelined()) {
      pipeline.del(keys.DATA + task.uniqueId());
      pipeline.zrem(keys.IN_PROGRESS_TASKS, task.taskString());
      pipeline.hdel(keys.TASK_RETRY_COUNT, task.uniqueId());
    }
  }

  Map<String, Double> peek(String delayedTaskQueue, int numberOfItems) {
    var r   = jedis.zrangeWithScores(delayedTaskQueue, 0, numberOfItems - 1);
    var map = new HashMap<String, Double>();
    r.forEach(e -> map.put(e.getElement(), e.getScore()));
    return map;
  }

  void addItemsFromDelayedTasksToQueue(List<String> tasks) {
    try (var transaction = jedis.multi()) {
      tasks.forEach(t -> {
        var task = new Task(t, eventParser);
        if (task.queueName() != null) {
          transaction.rpush(task.queueName(), task.taskString());
        } else {
          transaction.rpush(queuesHolder.defaultQueue, task.taskString());
        }
      });
      transaction.zrem(keys.SCHEDULED_TASK_QUEUE, tasks.toArray(new String[0]));
      transaction.exec();
    }
  }

  List<String> getAllItemsFromList(String listName) {
    return jedis.lrange(listName, 0, -1);
  }

  void deleteFromRotationListAndAddToQueue(List<String> items) {
    try (var transaction = jedis.multi()) {
      items.forEach(t -> {
        var task = new Task(t, eventParser);
        if (task.queueName() != null) {
          transaction.rpush(task.queueName(), task.taskString());
        } else {
          transaction.rpush(queuesHolder.defaultQueue, task.taskString());
        }
        transaction.lrem(keys.TEMP_ROTATION_LIST, 1, task.taskString());
      });
      transaction.exec();
    }
  }

  public long getNumberOfTaskInProgress() {
    return jedis.zcard(keys.IN_PROGRESS_TASKS);
  }

  ScanResult<Tuple> scanSortedSet(String key, String cursor) {
    return jedis.zscan(key, cursor);
  }

  ScanResult<Map.Entry<String, String>> scanHashSet(String key, String cursor) {
    return jedis.hscan(key, cursor, new ScanParams().count(200));
  }

  void deleteFromInProgressQueueAndAddToQueue(
      List<Task> tasksToRetry, List<Task> tasksToDelete) {
    if (tasksToRetry.isEmpty() && tasksToDelete.isEmpty()) return;

    if (!tasksToRetry.isEmpty()) {
      try (var transaction = jedis.multi()) {
        tasksToRetry.forEach(t -> {
          transaction.zadd(keys.SCHEDULED_TASK_QUEUE, t.executeAtTimeStamp().getEpochSecond(),
                           t.taskString());
          transaction.zrem(keys.IN_PROGRESS_TASKS, t.taskString());
        });
        transaction.exec();
      }
    }

    if (tasksToDelete.isEmpty()) return;
    try (var transaction = jedis.multi()) {
      tasksToDelete.forEach(t -> {
        transaction.zadd(keys.DEAD_TASKS, Instant.now().getEpochSecond(), t.taskString());
        transaction.zrem(keys.IN_PROGRESS_TASKS, t.taskString());
        transaction.expire(keys.DATA + t.uniqueId(), THC.Time.T_30_DAYS);
        transaction.hdel(keys.TASK_RETRY_COUNT, t.uniqueId());
      });
    }
  }

  void moveToDeadQueue(Task task) {
    try (var transaction = jedis.multi()) {
      transaction.zadd(keys.DEAD_TASKS, Instant.now().getEpochSecond(), task.taskString());
      transaction.expire(keys.DATA + task.uniqueId(), THC.Time.T_30_DAYS);
      transaction.zrem(keys.IN_PROGRESS_TASKS, task.taskString());
      transaction.hdel(keys.TASK_RETRY_COUNT, task.uniqueId());
      transaction.exec();
    }
  }

  void removeTasksFromSortedSetBasedOnTime(String key, Instant time) {
    if (!StringUtils.equalsAny(key, keys.DEAD_TASKS)) {
      throw new OperationNotAllowed(
          "This method is only allowed to delete tasks from the dead queue");
    }

    Supplier<List<String>> tasksSupplier =
        () -> jedis.zrangeByScore(key, 0, time.getEpochSecond(), 0, 100);
    do {
      var tasks = tasksSupplier.get();
      if (ObjectUtils.isEmpty(tasks)) break;
      tasks.forEach(t -> {
        var task = new Task(t, eventParser);
        try (var transaction = jedis.multi()) {
          transaction.zrem(key, task.taskString());
          transaction.del(keys.DATA + task.uniqueId());
          transaction.exec();
        }
      });
    } while (true);
  }
}
