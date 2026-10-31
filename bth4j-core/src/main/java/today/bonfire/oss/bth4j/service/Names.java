package today.bonfire.oss.bth4j.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskStore;

/**
 * Resolves queue and event names to the ids the store keeps, and back. Ids are permanent, so both
 * directions are cached for the life of the process; a reverse miss (an id another process registered)
 * reloads the registry once.
 */
public final class Names {

    private final TaskStore store;
    private final Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private final Map<String, Integer> eventIds = new ConcurrentHashMap<>();
    private final Map<Integer, String> queueNames = new ConcurrentHashMap<>();
    private final Map<Integer, String> eventNames = new ConcurrentHashMap<>();

    public Names(TaskStore store) {
        this.store = store;
    }

    public int queueId(String name) {
        return queueIds.computeIfAbsent(name, n -> {
            int id = store.queueId(n);
            queueNames.put(id, n);
            return id;
        });
    }

    public int eventId(String name) {
        return eventIds.computeIfAbsent(name, n -> {
            int id = store.eventId(n);
            eventNames.put(id, n);
            return id;
        });
    }

    public String queueName(int id) {
        var name = queueNames.get(id);
        if (name == null) {
            store.queueNames().forEach((i, n) -> {
                queueNames.put(i, n);
                queueIds.put(n, i);
            });
            name = queueNames.get(id);
        }
        return name == null ? "#" + id : name;
    }

    public String eventName(int id) {
        var name = eventNames.get(id);
        if (name == null) {
            store.eventNames().forEach((i, n) -> {
                eventNames.put(i, n);
                eventIds.put(n, i);
            });
            name = eventNames.get(id);
        }
        return name == null ? "#" + id : name;
    }

    public TaskView view(TaskRecord record) {
        return new TaskView(record, queueName(record.queueId()), eventName(record.eventId()));
    }
}
