package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/** The argument rules of the store SPI, shared by the store implementations so that every store refuses alike. */
public final class StoreArguments {

    private StoreArguments() {}

    /** @throws TaskValidationException when {@code count} is below 1 */
    public static void requireCount(int count) {
        if (count < 1) throw new TaskValidationException("count must be positive: " + count);
    }

    /** @throws TaskValidationException when {@code limit} is below 1 */
    public static void requireLimit(int limit) {
        if (limit < 1) throw new TaskValidationException("limit must be positive: " + limit);
    }

    /** @throws TaskValidationException when the queue or type name is null or blank */
    public static void requireName(String kind, String name) {
        if (name == null || name.isBlank()) throw new TaskValidationException(kind + " name is required");
    }

    /** @throws TaskValidationException when the unique key is empty; null is a task without a key */
    public static void requireKey(String uniqueKey) {
        if (uniqueKey != null && uniqueKey.isEmpty()) throw new TaskValidationException("unique key is empty");
    }

    /** @throws TaskValidationException when the worker label is null */
    public static void requireLabel(String label) {
        if (label == null) throw new TaskValidationException("label is required");
    }

    /** @throws TaskValidationException when the due time is null */
    public static void requireDue(Instant due) {
        if (due == null) throw new TaskValidationException("due is required");
    }

    /** @throws TaskValidationException when the state is not terminal */
    public static void requireTerminal(TaskState state) {
        if (!state.isTerminal()) throw new TaskValidationException("only terminal states are purged: " + state);
    }
}
