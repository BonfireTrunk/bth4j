package today.bonfire.oss.bth4j;

import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskStore;

/**
 * Resolves queue and type names to the ids the store keeps, and back. Ids are permanent, so both
 * directions are cached for the life of the process; a reverse miss (an id another process registered)
 * reloads the registry once.
 */
final class Names {

    /** What an event says for a queue or type it cannot name. */
    static final String UNKNOWN = "unknown";

    private final TaskStore store;
    private final Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private final Map<String, Integer> typeIds = new ConcurrentHashMap<>();
    private final Map<Integer, String> queueNames = new ConcurrentHashMap<>();
    private final Map<Integer, String> typeNames = new ConcurrentHashMap<>();

    Names(TaskStore store) {
        this.store = store;
    }

    int queueId(String name) {
        return queueIds.computeIfAbsent(name, n -> {
            int id = store.queueId(n);
            queueNames.put(id, n);
            return id;
        });
    }

    /** Looks up a queue the store already knows, reloading its registry once on a miss; registers nothing. */
    OptionalInt knownQueueId(String name) {
        var id = queueIds.get(name);
        if (id == null) {
            store.queueNames().forEach((i, n) -> {
                queueNames.put(i, n);
                queueIds.put(n, i);
            });
            id = queueIds.get(name);
        }
        return id == null ? OptionalInt.empty() : OptionalInt.of(id);
    }

    int typeId(String name) {
        return typeIds.computeIfAbsent(name, n -> {
            int id = store.typeId(n);
            typeNames.put(id, n);
            return id;
        });
    }

    String queueName(int id) {
        var name = knownQueueName(id);
        return name == null ? "#" + id : name;
    }

    String typeName(int id) {
        var name = knownTypeName(id);
        return name == null ? "#" + id : name;
    }

    /** The queue name for an event, or {@link #UNKNOWN} when the id is 0 (a store could not read it) or unregistered. */
    String queueNameOrUnknown(int id) {
        var name = knownQueueName(id);
        return name == null ? UNKNOWN : name;
    }

    /** The type name for an event, or {@link #UNKNOWN} when the id is 0 (a store could not read it) or unregistered. */
    String typeNameOrUnknown(int id) {
        var name = knownTypeName(id);
        return name == null ? UNKNOWN : name;
    }

    private String knownQueueName(int id) {
        var name = queueNames.get(id);
        if (name == null && id > 0) {
            store.queueNames().forEach((i, n) -> {
                queueNames.put(i, n);
                queueIds.put(n, i);
            });
            name = queueNames.get(id);
        }
        return name;
    }

    private String knownTypeName(int id) {
        var name = typeNames.get(id);
        if (name == null && id > 0) {
            store.typeNames().forEach((i, n) -> {
                typeNames.put(i, n);
                typeIds.put(n, i);
            });
            name = typeNames.get(id);
        }
        return name;
    }

    TaskView view(TaskRecord record) {
        return new TaskView(record, queueName(record.queueId()), typeName(record.typeId()));
    }
}
