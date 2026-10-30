package today.bonfire.oss.bth4j.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.TaskProcessor;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

/**
 * Registry for task processors that manages the mapping between task events and their processors.
 * This is an internal component of the library that handles the execution of tasks with their
 * appropriate processors.
 */
public class TaskProcessorRegistry {
    private static final Logger log = LoggerFactory.getLogger(TaskProcessorRegistry.class);

    private final Map<String, TaskProcessor<?>> processors;

    public TaskProcessorRegistry() {
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
        processors.put(event.value(), processor);
        log.debug(
                "Registered processor for event: {} with data type: {}",
                event,
                processor.dataTypeClass().getSimpleName());
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
        var event = task.event();
        TaskProcessor<?> processor = processors.get(event.value());
        if (processor == null) {
            throw new TaskConfigurationError("No processor registered for event: " + event);
        }
        return processor;
    }

    /**
     * Execute a task using its registered processor. The data carried by the claimed task is decoded to the
     * processor's data class with the given function.
     */
    void executeTask(Task task, BiFunction<Task, Class<?>, ?> decoder) {
        executeWithProcessor(task, getProcessor(task), decoder);
    }

    @SuppressWarnings("unchecked")
    private <T> void executeWithProcessor(
            Task task, TaskProcessor<?> processor, BiFunction<Task, Class<?>, T> decoder) {
        TaskProcessor<T> typedProcessor = (TaskProcessor<T>) processor;
        if (typedProcessor.requiresData()) {
            typedProcessor.process(task, decoder.apply(task, typedProcessor.dataTypeClass()));
        } else {
            typedProcessor.process(task, null);
        }
    }
}
