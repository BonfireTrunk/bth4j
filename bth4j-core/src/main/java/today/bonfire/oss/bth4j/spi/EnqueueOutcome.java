package today.bonfire.oss.bth4j.spi;

/** Result of {@link ExecutionStore#enqueue}. */
public sealed interface EnqueueOutcome {

    /** the task was stored */
    record Stored() implements EnqueueOutcome {}

    /** a task with that id already exists in some state; nothing was changed */
    record Exists() implements EnqueueOutcome {}

    /** the unique key belongs to another task; nothing was changed */
    record DuplicateKey(long existingId) implements EnqueueOutcome {}

    EnqueueOutcome STORED = new Stored();
    EnqueueOutcome EXISTS = new Exists();
}
