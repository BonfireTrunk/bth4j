package today.bonfire.oss.bth4j;

import java.time.Duration;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * How long a {@link TaskRequest#uniqueKey()} stays taken. At most one task record holds the key; an old
 * processor can outlive its lease and overlap a replacement until it checks or responds to an opted-in
 * interrupt at an interruptible blocking call. Fence side effects with the revision or an idempotent
 * target, and use steps to checkpoint completed work.
 */
public sealed interface Uniqueness {

    /** The key is released as soon as the task reaches a terminal state. */
    record UntilDone() implements Uniqueness {}

    /**
     * The key stays taken for this long after the task reaches a terminal state.
     *
     * @param duration at least one millisecond, which is the finest a store keeps, and at most {@link #MAX_KEEP_FOR}
     */
    record KeepFor(Duration duration) implements Uniqueness {
        public KeepFor {
            if (duration == null || duration.toMillis() < 1) {
                throw new TaskValidationException("duration must be at least 1 ms");
            }
            if (duration.compareTo(MAX_KEEP_FOR) > 0) {
                throw new TaskValidationException("duration must be at most " + MAX_KEEP_FOR);
            }
        }
    }

    /**
     * The longest {@link KeepFor} window, {@link TaskRequest#MAX_TIME_FROM_NOW} (100 years): Redis cannot expire a
     * key much later.
     */
    Duration MAX_KEEP_FOR = TaskRequest.MAX_TIME_FROM_NOW;

    Uniqueness UNTIL_DONE = new UntilDone();

    static Uniqueness keepFor(Duration duration) {
        return new KeepFor(duration);
    }
}
