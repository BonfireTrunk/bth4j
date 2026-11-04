package today.bonfire.oss.bth4j.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskStore;

/**
 * Resolves queue and type names to the ids the store keeps, and back. Ids are permanent, so both
 * directions are cached for the life of the process; a reverse miss (an id another process registered)
 * reloads the registry once.
 */
public final class Names {

    private final TaskStore store;
    private final Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private final Map<String, Integer> typeIds = new ConcurrentHashMap<>();
    private final Map<Integer, String> queueNames = new ConcurrentHashMap<>();
    private final Map<Integer, String> typeNames = new ConcurrentHashMap<>();

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

    public int typeId(String name) {
        return typeIds.computeIfAbsent(name, n -> {
            int id = store.typeId(n);
            typeNames.put(id, n);
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

    public String typeName(int id) {
        var name = typeNames.get(id);
        if (name == null) {
            store.typeNames().forEach((i, n) -> {
                typeNames.put(i, n);
                typeIds.put(n, i);
            });
            name = typeNames.get(id);
        }
        return name == null ? "#" + id : name;
    }

    public TaskView view(TaskRecord record) {
        return new TaskView(record, queueName(record.queueId()), typeName(record.typeId()));
    }
}
