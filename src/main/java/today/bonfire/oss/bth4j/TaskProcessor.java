package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.service.Task;

/**
 * Core interface for processing tasks in the background task handler.
 * Implementations should provide the specific logic for processing different types of tasks.
 *
 * @param <T> The type of data associated with the task. Use {@link Void} for tasks that don't require data.
 */
public interface TaskProcessor<T> {

  /**
   * Process the given task with its associated data.
   *
   * @param task The task to process
   * @param data The data associated with the task. Will be null if {@link #requiresData()} returns false.
   */
  void process(Task task, T data);

  /**
   * Get the class type of data this processor expects.
   * This is used for automatic deserialization of task data.
   * <p>
   * if your task needs data then make sure to implement this method.
   *
   * @return The class type of data this processor handles
   */
  default Class<?> dataTypeClass() {
    return Void.class;
  }

  /**
   * Indicates whether this processor requires data to process the task.
   * Default implementation returns false if void class type i.e. no data;
   *
   * @return true if the processor needs data, false otherwise
   */
  default boolean requiresData() {
    return Void.class != dataTypeClass();
  }
}
