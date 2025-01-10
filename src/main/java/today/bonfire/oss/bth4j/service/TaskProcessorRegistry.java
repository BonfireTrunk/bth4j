package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.TaskProcessor;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Registry for task processors that manages the mapping between task events and their processors.
 * This is an internal component of the library that handles the execution of tasks with their
 * appropriate processors.
 */
@Slf4j
public class TaskProcessorRegistry {

  private final Map<String, TaskProcessor<?>> processors;

  public TaskProcessorRegistry() {
    // Using raw Enum<?> type to support any enum type for events
    this.processors = new ConcurrentHashMap<>();
  }

  /**
   * Register a processor for a specific event type.
   *
   * @param event     The event enum value to register the processor for
   * @param processor The processor to handle tasks of this event type
   * @param <E>       The enum type of the event
   * @param <T>       The type of data the processor handles
   */
  public <E extends Event, T> void register(E event, TaskProcessor<T> processor) {
    processors.put(event.toString(), processor);
    log.debug("Registered processor for event: {} with data type: {}",
              event, processor.dataTypeClass().getSimpleName());
  }

  /**
   * Get the processor registered for a specific task.
   *
   * @param task The task to get the processor for
   * @return The registered processor for the task's event type
   *
   * @throws TaskConfigurationError if no processor is registered for the task's event
   */
  TaskProcessor<?> getProcessor(Task task) {
    var              event     = task.event();
    TaskProcessor<?> processor = processors.get(event.toString());
    if (processor == null) {
      throw new TaskConfigurationError("No processor registered for event: " + event);
    }
    return processor;
  }

  /**
   * Execute a task using its registered processor.
   *
   * @param task The task to execute
   */
  public void executeTask(Task task, BiFunction<String, Class<?>, ?> function) {
    executeWithProcessor(task, getProcessor(task), function);
  }

  @SuppressWarnings("unchecked")
  private <T> void executeWithProcessor(Task task, TaskProcessor<?> processor, BiFunction<String, Class<?>, T> function) {
    TaskProcessor<T> typedProcessor = (TaskProcessor<T>) processor;
    if (typedProcessor.requiresData()) {
      T data = function.apply(task.uniqueId(), typedProcessor.dataTypeClass());
      typedProcessor.process(task, data);
    } else {
      typedProcessor.process(task, null);
    }
  }
}
