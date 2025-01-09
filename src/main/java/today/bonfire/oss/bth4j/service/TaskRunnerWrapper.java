package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.Task;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;

import java.util.function.Consumer;

@Slf4j
public class TaskRunnerWrapper implements Runnable {

  private final Task           task;
  private final Consumer<Task> taskHandler;
  private final TaskCallbacks  callbacks;

  TaskRunnerWrapper(
      Task task,
      Consumer<Task> taskHandler,
      TaskCallbacks callbacks) {
    this.task        = task;
    this.taskHandler = taskHandler;
    this.callbacks   = callbacks;
  }

  /**
   * if task fails it may be retired automatically by maintenance service
   * unless the exception is UnrecoverableException in which case the task is marked
   * dead and deleted.
   */
  @Override
  public void run() {
    try {
      callbacks.beforeStart().accept(task);
      taskHandler.accept(task);
      deleteTask(task);
      callbacks.onSuccess().accept(task);
    } catch (Exception e) {
      callbacks.onError().accept(task, e);
      log.error("Task {} failed", task.taskString(), e);
      if (e instanceof TaskUnrecoverableException || e instanceof TaskDataException) {
        // task will not be retried
        moveToDeadQueue(task);
      } else if (e instanceof TaskErrorException) {
        // task can be retried
        log.info("Task will be retried");
      } else if (e instanceof TaskRescheduleException ex) {
        rescheduleTask(task, ex.delay());
      } else {
        // unknown exception task can be retried
        log.info("Unhandled exception. Task will be retried");
      }
    } finally {
      callbacks.afterTask().accept(task);
    }
  }

  private void rescheduleTask(Task task, long delay) {
    var newTask = Task.Builder.newTask()
                              .event(task.event())
                              .accountId(task.accountId())
                              .queueName(task.queueName())
                              .executeAfter(delay)
                              .build();
    TaskOps.addTaskToQueue(newTask, TaskOps.getDataForTask(task.uniqueId()), true);
    log.info("Rescheduling old task {} to run after {} seconds, new task id {}",
             task.uniqueId(), delay, newTask.uniqueId());
    deleteTask(task);
  }

  private void deleteTask(Task task) {
    // delete the task and data if present
    TaskOps.deleteTaskFromInProgressQueue(task);
  }

  private void moveToDeadQueue(Task task) {
    // move to dead list if task failed because of BGTaskUnrecoverableException
    TaskOps.moveToDeadQueue(task);
  }
}
