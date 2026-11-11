package today.bonfire.oss.bth4j.spi;

import java.util.Map;
import java.util.Set;

/**
 * What every store provides: identity, the name registries and schema management. The task operations are
 * split into {@link ExecutionStore}, {@link MaintenanceStore}, {@link ScheduleStore} and
 * {@link InspectionStore}; a store module implements all of them (see {@link FullStore}).
 * <p>
 * All methods are called concurrently, from several threads of one process and from several processes
 * sharing the store. They throw {@link TaskStoreException} when the store cannot be reached; callers retry
 * on their next cycle. The store's own clock decides lease expiry, due times and retention; the client
 * clock only sets {@link TaskRecord#createdAt()} and request-level instants.
 * <p>
 * Every instant the library hands a store lies within {@link today.bonfire.oss.bth4j.TaskRequest#MAX_TIME_FROM_NOW}
 * (100 years) of now, and every duration (a lease, a key window, a retention, a stale-after) is at most that
 * long, so a store may keep times as epoch milliseconds or SQL timestamps. A store keeps times and durations in
 * whole milliseconds: it drops what is finer from those it is given, takes its own clock to the millisecond, and
 * compares a time it is given with a stored one to the millisecond. Arguments that break a rule of this
 * SPI throw {@link IllegalArgumentException}; {@link StoreArguments} holds the shared checks.
 */
public interface TaskStore {

    StoreInfo info();

    /**
     * Creates the schema when it is missing, or verifies that the existing one has the version
     * {@link #info()} reports.
     *
     * @throws TaskStoreException when the stored schema version differs
     * @throws today.bonfire.oss.bth4j.exceptions.TaskConfigurationException when the engine is not set up as the
     *     store needs it, for example a Redis server that evicts keys or an SQLite connection that is not in WAL mode
     */
    void init();

    /**
     * Reserves {@code count} task ids. Ids are unique within the store, never reused, and not ordered in time
     * across processes.
     *
     * @param count at least 1
     * @return the first id; the caller owns {@code [first, first + count)}
     */
    long allocateIds(int count);

    /**
     * Registers an accepting worker and returns its id. Ids are never reused.
     *
     * @param label free text that identifies the process, for example host and pid; not null
     * @param queueIds queues this worker processes
     * @param typeIds task types this worker processes
     */
    int allocateWorkerId(String label, Set<Integer> queueIds, Set<Integer> typeIds);

    /**
     * Resolves a queue name to its id, registering the name when it is new. Ids are permanent, so callers
     * cache them for the life of the process.
     *
     * @param name not null or blank
     */
    int queueId(String name);

    /**
     * Resolves a task type name to its id, registering it when new. Permanent, cacheable.
     *
     * @param name not null or blank
     */
    int typeId(String name);

    /** @return every registered queue, id to name */
    Map<Integer, String> queueNames();

    /** @return every registered type, id to name */
    Map<Integer, String> typeNames();
}
