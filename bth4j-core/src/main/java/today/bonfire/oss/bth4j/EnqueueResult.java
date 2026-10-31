package today.bonfire.oss.bth4j;

/** Result of {@link TaskClient#enqueue}. */
public sealed interface EnqueueResult {

    /** The task is stored under this id (or already was, when the same id was enqueued before). */
    record Accepted(long id) implements EnqueueResult {}

    /** Another task holds the unique key; nothing was stored. */
    record Duplicate(long existingId) implements EnqueueResult {}

    /** @return the id of the stored task, or of the task that holds the key */
    default long id() {
        return switch (this) {
            case Accepted a -> a.id();
            case Duplicate d -> d.existingId();
        };
    }

    default boolean accepted() {
        return this instanceof Accepted;
    }
}
