package today.bonfire.oss.bth4j;

import java.time.Duration;

/** How long a {@link TaskRequest#uniqueKey()} stays taken. */
public sealed interface Uniqueness {

    /** The key is released as soon as the task reaches a terminal state. */
    record UntilDone() implements Uniqueness {}

    /** The key stays taken for this long after the task reaches a terminal state. */
    record KeepFor(Duration duration) implements Uniqueness {
        public KeepFor {
            if (duration == null || duration.isNegative() || duration.isZero()) {
                throw new IllegalArgumentException("duration must be positive");
            }
        }
    }

    Uniqueness UNTIL_DONE = new UntilDone();

    static Uniqueness keepFor(Duration duration) {
        return new KeepFor(duration);
    }
}
