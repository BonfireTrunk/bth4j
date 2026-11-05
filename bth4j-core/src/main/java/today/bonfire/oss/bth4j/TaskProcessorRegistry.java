package today.bonfire.oss.bth4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/**
 * Maps task types to their processors. Register everything before the runner starts; the registry is
 * frozen then, so the set of types a runner claims is fixed for its life.
 */
public final class TaskProcessorRegistry {

    /** A registered type with its processor. */
    public record Registration<T>(TaskType<T> type, TaskProcessor<T> processor) {}

    private final Map<String, Registration<?>> registrations = new ConcurrentHashMap<>();
    private volatile boolean frozen;

    /** @throws TaskConfigurationException when the type name is already registered or the registry is frozen */
    public <T> TaskProcessorRegistry register(TaskType<T> type, TaskProcessor<T> processor) {
        if (type == null || processor == null) throw new TaskConfigurationException("type and processor are required");
        if (frozen) throw new TaskConfigurationException("The runner is started; register processors before start()");
        if (registrations.putIfAbsent(type.name(), new Registration<>(type, processor)) != null) {
            throw new TaskConfigurationException("A processor for task type " + type + " is already registered");
        }
        return this;
    }

    public Registration<?> get(String typeName) {
        return registrations.get(typeName);
    }

    public Set<String> typeNames() {
        return Set.copyOf(registrations.keySet());
    }

    public boolean isEmpty() {
        return registrations.isEmpty();
    }

    void freeze() {
        frozen = true;
    }
}
