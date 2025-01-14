# Background Task Handler (BTH4J)

This Java library provides a robust and flexible framework for managing background tasks. It leverages Redis for persistent task storage and queue management, ensuring reliability and scalability.

## Overview

BTH4J is designed to handle various types of tasks:

* **Regular Tasks:** Tasks executed once and then removed from the queue.
* **Scheduled Tasks:** Tasks scheduled to run at a specific future time.
* **Recurring Tasks:** Tasks executed periodically at defined intervals.

The library employs a multi-threaded architecture with configurable parameters for queue processing intervals, maintenance checks, and retry mechanisms. It also supports custom callbacks for task lifecycle events (before start, success, error, after task).

## Installation

Add the following dependency to your project:

```xml
<dependency>
    <groupId>today.bonfire.oss</groupId>
    <artifactId>bth4j</artifactId>
    <version>2.3.0</version>
</dependency>
```

## Architecture

The core components of BTH4J are:

1. **BackgroundRunner:** The main orchestrator, responsible for initializing and managing all background threads and services. It monitors the health of these services and restarts them if necessary.

3. **TaskProcessorService:** A service that continuously monitors task queues, retrieves tasks, and submits them to the `taskExecutor` for processing. It uses a round-robin approach to process multiple queues.

4. **TaskProcessorRegistry:** A registry that maps task events to their corresponding processors. This allows for flexible task processing logic based on the task type.

5. **TaskProcessor:** An interface defining the contract for processing individual tasks. Custom implementations can be provided to handle specific task types.

6. **TaskRunnerWrapper:** A wrapper class that executes a task and handles callbacks, error handling, and retry logic.

7. **ScheduledTaskService:** A service and handler responsible for managing delayed tasks. It periodically checks for tasks that are ready to be executed and moves them to the appropriate queue.

8. **RecurringTaskService & RecurringTaskRunnerWrapper:** Services and wrappers responsible for managing recurring tasks. It periodically checks for recurring tasks and adds them to the queue based on their schedule.

9. **MaintenanceService:** A service that performs periodic maintenance tasks, such as checking for stale tasks (tasks that have been in progress for too long), removing expired locks, and retrying failed tasks.

10. **TaskOps:** A utility class providing operations for interacting with Redis, such as adding tasks to queues, retrieving data, managing locks, and performing other Redis-related operations.

11. **TaskCallbacks:** A record class that encapsulates callback functions for task lifecycle events.

**Architecture Diagram:**

```
                                +----------------------+
                                |   BackgroundRunner   |
                                +----------------------+
                                          |
                    +---------------------+---------------------+
                    |                     |                    |
                    v                     v                    v
        +-----------------+    +------------------+   +-----------------+
        |TaskProcessorSvc |    |ScheduledTaskSvc  |   |RecurringTaskSvc |
        +-----------------+    +------------------+   +-----------------+
                |                      |                      |
                |                      |                      |
                v                      v                      v
        +-----------------+    +-----------------+    +-----------------+
        |TaskRunnerWrapper|    |     TaskOps     |<---|MaintenanceService|
        +-----------------+    +-----------------+    +-----------------+
                |                      ^
                |                      |
                v                      |
        +-----------------+           |
        |BackgroundExecutor|          |
        |  - VtExecutor   |          |
        |  - PtExecutor   |          |
        +-----------------+          |
                |                    |
                v                    |
        +-----------------+         |
        | TaskProcessor<T>|         |
        +-----------------+         |
                |                   |
                +-------------------+

        Data Flow:
        ----------
        1. BackgroundRunner orchestrates all services
        2. TaskProcessorSvc processes tasks through executor
        3. ScheduledTaskSvc manages delayed tasks
        4. RecurringTaskSvc handles periodic tasks
        5. MaintenanceService monitors and maintains system health
        6. All services interact with TaskOps for Redis operations
        7. TaskRunnerWrapper handles task execution lifecycle
        8. Processors implement actual task business logic
```

## Usage Examples

### 1. Setting Up Background Runner

```java
// Configure Redis connection
var poolConfig = JedisPoolConfig.builder()
    .maxPoolSize(1000)
    .minPoolSize(1)
    .testOnBorrow(true)
    .testWhileIdle(true)
    .objEvictionTimeout(Duration.ofSeconds(5))
    .build();

var clientConfig = DefaultJedisClientConfig.builder()
    .resp3()
    .timeoutMillis(1000)
    .clientName("bth4j-app")
    .user("app")
    .password("your-password")
    .build();

var jedis = new JedisPooled(new HostAndPort("localhost", 6379), clientConfig, poolConfig.build());

// Create and configure BackgroundRunner
var backgroundRunner = new BackgroundRunner.Builder()
    .taskProcessorRegistry(registry)
    .eventParser(YourEvent.UNKNOWN::from)  // Your event parser
    .taskRetryDelay(Integer::valueOf)
    .taskProcessorQueueCheckInterval(Duration.ofMillis(100))
    .maintenanceCheckInterval(Duration.ofSeconds(30))
    .staleTaskTimeout(Duration.ofMinutes(5))
    .jsonMapper(yourJsonMapper)
    .jedisClient(jedis)
    .namespacePrefix("your-app")
    .taskExecutor(new DefaultVtExecutor())
    .build();

// Start the background runner
Thread bgThread = new Thread(backgroundRunner);
bgThread.start();
```

### 2. Creating Task Processors

```java
// Example task processor that requires data
public class DataProcessor implements TaskProcessor<YourDataType> {
    @Override 
    public void process(Task task, YourDataType data) {
        // Process your task with data
    }

    @Override 
    public Class<YourDataType> dataTypeClass() {
        return YourDataType.class;
    }

    @Override 
    public boolean requiresData() {
        return true;
    }
}

// Example task processor without data requirement
public class NoDataProcessor implements TaskProcessor<Void> {
    @Override 
    public void process(Task task, Void data) {
        // Process your task
    }
}
```

### 3. Registering Task Processors

```java
TaskProcessorRegistry registry = new TaskProcessorRegistry();
registry.register(YourEvents.EVENT_TYPE, new DataProcessor());
registry.register(YourEvents.NO_DATA_EVENT, new NoDataProcessor());
```

### 4. Creating and Queueing Tasks

```java
// Regular task
var task = Task.Builder.newTask()
    .accountId("account123")
    .event(YourEvents.EVENT_TYPE)
    .build();
taskOps.addTaskToQueue(task, yourData);

// Delayed task (execute after 5 seconds)
var delayedTask = Task.Builder.newTask()
    .accountId("account123")
    .event(YourEvents.EVENT_TYPE)
    .executeAfter(5)
    .build();
taskOps.addTaskToQueue(delayedTask, yourData);

// Recurring task with cron expression
var recurringTask = Task.Builder.newTask()
    .accountId("account123")
    .event(YourEvents.RECURRING)
    .cronExpression("0 */15 * * * *")  // Every 15 minutes
    .build();
taskOps.addRecurringTask(recurringTask);
```

### 5. Task Lifecycle Callbacks

```java
var callbacks = new TaskCallbacks(
    task -> log.info("Before start: {}", task),  // Before task starts
    task -> log.info("Success: {}", task),       // On success
    (task, ex) -> log.error("Error: {}", ex),    // On error
    task -> log.info("After task: {}", task)     // After task completion
);

// Use callbacks in BackgroundRunner configuration
.setRegularTaskCallbacks(callbacks)
.setRecurringTaskCallbacks(callbacks)
```

## Configuration Options

### BackgroundRunner Configuration
- `taskProcessorQueueCheckInterval`: Interval to check for new tasks (default: 100ms)
- `maintenanceCheckInterval`: Interval for maintenance operations (default: 30s)
- `staleTaskTimeout`: Time after which a task is considered stale (default: 5m)
- `taskRetryDelay`: Delay before retrying failed tasks
- `namespacePrefix`: Prefix for Redis keys to avoid conflicts
- `BGThreadCheckInterval`: Interval for background thread health checks
- `delayedTasksQueueCheckInterval`: Interval for checking delayed tasks
- `recurringTasksQueueCheckInterval`: Interval for checking recurring tasks
- `queueTaskAheadDurationForRecurringTasks`: Duration to queue recurring tasks ahead of time

### Redis Configuration
- Connection pooling settings
- Connection timeout
- Authentication credentials
- Client name for monitoring

## Best Practices

1. **Task Design**
   - Keep tasks small and focused
   - Ensure idempotency when possible
   - Include necessary context in task data
   - Use meaningful event names and account IDs

2. **Error Handling**
   - Implement proper error handling in processors
   - Use callbacks for monitoring task execution
   - Configure appropriate retry policies
   - Handle task failures gracefully

3. **Performance**
   - Monitor Redis connection pool usage
   - Adjust queue check intervals based on load
   - Use appropriate task timeout values
   - Configure thread pool sizes based on workload

4. **Monitoring**
   - Implement logging in task processors
   - Monitor stale tasks and failures
   - Track task execution times
   - Set up alerts for critical failures

## Limitations

- Cron expressions follow Unix crontab format
- Redis/valkey or similar key value db is required for operation
- Virtual thread support requires Java 21 or later. This library itself has baseline java requirement of Java 21.
