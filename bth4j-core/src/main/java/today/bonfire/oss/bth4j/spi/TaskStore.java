package today.bonfire.oss.bth4j.spi;

import java.util.Map;

/**
 * What every store provides: identity, the name registries and schema management. The task operations are
 * split into {@link ExecutionStore}, {@link MaintenanceStore}, {@link ScheduleStore} and
 * {@link InspectionStore}; a store module implements all of them (see {@link FullStore}).
 * <p>
 * All methods are called concurrently, from several threads of one process and from several processes
 * sharing the store. They throw {@link TaskStoreException} when the store cannot be reached; callers retry
 * on their next cycle. The store's own clock decides lease expiry, due times and retention; the client
 * clock only sets {@link TaskRecord#created()} and request-level instants.
 */
public interface TaskStore {

    StoreInfo info();

    /**
     * Creates the schema when it is missing, or verifies that the existing one has
     * {@link StoreInfo#SCHEMA_VERSION}.
     *
     * @throws TaskStoreException when the stored schema version differs
     */
    void init();

    /**
     * Reserves {@code count} task ids. Ids are unique within the store, never reused, and not ordered in time
     * across processes.
     *
     * @return the first id; the caller owns {@code [first, first + count)}
     */
    long allocateIds(int count);

    /**
     * Registers a worker and returns its id. Ids are never reused.
     *
     * @param label free text that identifies the process, for example host and pid
     */
    int allocateWorkerId(String label);

    /**
     * Resolves a queue name to its id, registering the name when it is new. Ids are permanent, so callers
     * cache them for the life of the process.
     */
    int queueId(String name);

    /** Resolves an event (task type) name to its id, registering it when new. Permanent, cacheable. */
    int eventId(String name);

    /** @return every registered queue, id to name */
    Map<Integer, String> queueNames();

    /** @return every registered event, id to name */
    Map<Integer, String> eventNames();
}
