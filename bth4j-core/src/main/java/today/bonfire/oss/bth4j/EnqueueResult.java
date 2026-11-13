package today.bonfire.oss.bth4j;

/** Result of {@link TaskClient#enqueue}. */
public sealed interface EnqueueResult {

    /**
     * The task is stored under this id.
     *
     * @param id      the id of the stored task
     * @param created true when this call stored the task, false when a task with this id and the same content
     *                was already there and nothing was written. A resend that the store's own transport repeated
     *                after its first write went through still answers true, as the store reports its own
     *                earlier write as a first write.
     */
    record Accepted(long id, boolean created) implements EnqueueResult {}

    /** Another task holds the unique key; nothing was stored. */
    record Duplicate(long existingId) implements EnqueueResult {}

    /** @return the id of the stored task, or of the task that holds the key */
    default long id() {
        return switch (this) {
            case Accepted a -> a.id();
            case Duplicate d -> d.existingId();
        };
    }

    default boolean isAccepted() {
        return this instanceof Accepted;
    }
}
