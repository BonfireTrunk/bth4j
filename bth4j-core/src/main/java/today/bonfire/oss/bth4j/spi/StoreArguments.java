package today.bonfire.oss.bth4j.spi;

import java.time.Instant;

/** The argument rules of the store SPI, shared by the store implementations so that every store refuses alike. */
public final class StoreArguments {

    private StoreArguments() {}

    /** @throws IllegalArgumentException when {@code count} is below 1 */
    public static void requireCount(int count) {
        if (count < 1) throw new IllegalArgumentException("count must be positive: " + count);
    }

    /** @throws IllegalArgumentException when {@code limit} is below 1 */
    public static void requireLimit(int limit) {
        if (limit < 1) throw new IllegalArgumentException("limit must be positive: " + limit);
    }

    /** @throws IllegalArgumentException when the queue or type name is null or blank */
    public static void requireName(String kind, String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException(kind + " name is required");
    }

    /** @throws IllegalArgumentException when the unique key is empty; null is a task without a key */
    public static void requireKey(String uniqueKey) {
        if (uniqueKey != null && uniqueKey.isEmpty()) throw new IllegalArgumentException("unique key is empty");
    }

    /** @throws IllegalArgumentException when the worker label is null */
    public static void requireLabel(String label) {
        if (label == null) throw new IllegalArgumentException("label is required");
    }

    /** @throws IllegalArgumentException when the due time is null */
    public static void requireDue(Instant due) {
        if (due == null) throw new IllegalArgumentException("due is required");
    }

    /** @throws IllegalArgumentException when the state is not terminal */
    public static void requireTerminal(TaskState state) {
        if (!state.isTerminal()) throw new IllegalArgumentException("only terminal states are purged: " + state);
    }
}
