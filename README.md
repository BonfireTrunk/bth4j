# Background Task Handler (BTH4J)

This Java library provides a robust and flexible framework for managing background tasks.  It leverages Redis for persistent task storage and queue management, ensuring reliability and scalability.

## Overview

BTH4J is designed to handle various types of tasks:

* **Regular Tasks:**  Tasks executed once and then removed from the queue.
* **Delayed Tasks:** Tasks scheduled to run at a specific future time.
* **Recurring Tasks:** Tasks executed periodically at defined intervals.

The library employs a multi-threaded architecture with configurable parameters for queue processing intervals, maintenance checks, and retry mechanisms.  It also supports custom callbacks for task lifecycle events (before start, success, error, after task).

## Architecture

The core components of BTH4J are:

1. **BackgroundRunner:** The main orchestrator, responsible for initializing and managing all background threads and services.  It monitors the health of these services and restarts them if necessary.

2. **BgrParent:**  A parent class providing common functionality and configuration for background services.  It manages the available queues and their processing status.

3. **TaskProcessorService:**  A service that continuously monitors task queues, retrieves tasks, and submits them to the `taskExecutor` for processing.  It uses a round-robin approach to process multiple queues.

4. **TaskProcessorRegistry:**  A registry that maps task events to their corresponding processors.  This allows for flexible task processing logic based on the task type.

5. **TaskProcessor:** An interface defining the contract for processing individual tasks.  Custom implementations can be provided to handle specific task types.

6. **TaskRunnerWrapper:** A wrapper class that executes a task and handles callbacks, error handling, and retry logic.

7. **DelayedTaskService & DelayedTaskHandler:**  A service and handler responsible for managing delayed tasks.  It periodically checks for tasks that are ready to be executed and moves them to the appropriate queue.

8. **RecurringTaskService & RecurringTaskRunnerWrapper:** Services and wrappers responsible for managing recurring tasks.  It periodically checks for recurring tasks and adds them to the queue based on their schedule.

9. **MaintenanceService:** A service that performs periodic maintenance tasks, such as checking for stale tasks (tasks that have been in progress for too long), removing expired locks, and retrying failed tasks.

10. **TaskOps:** A utility class providing operations for interacting with Redis, such as adding tasks to queues, retrieving data, managing locks, and performing other Redis-related operations.

11. **TaskCallbacks:** A record class that encapsulates callback functions for task lifecycle events.


**Simplified Architecture Diagram:**

```
+-----------------+     +-----------------+     +-----------------+
| BackgroundRunner |---->| TaskProcessorSvc |---->| TaskExecutor    |
+-----------------+     +-----------------+     +-----------------+
      ^                                         |
      |                                         v
      +---------------------------------------+-----------------+
      |                                         | TaskRunnerWrapper |
      |                                         +-----------------+
      |     +-----------------+     +-----------------+
      +---->| DelayedTaskSvc  |---->| DelayedTaskHndlr|
            +-----------------+     +-----------------+
      |     +-----------------+     +-----------------+
      +---->| RecurringTaskSvc|---->| RecurringTaskWrpr|
            +-----------------+     +-----------------+
      |     +-----------------+
      +---->| MaintenanceSvc  |
            +-----------------+
```

## Usage

Refer to the individual class documentation for detailed usage instructions.  The library is designed to be highly configurable, allowing you to customize various aspects of task processing to suit your specific needs.
