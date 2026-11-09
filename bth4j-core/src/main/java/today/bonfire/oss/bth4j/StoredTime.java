package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * The rule for times the library hands to a store: an instant lies within {@link TaskRequest#MAX_TIME_FROM_NOW}
 * of now and a duration is at most that long, so that every store can keep it. A time a producer, an operator or
 * the configuration supplies is refused beyond it; a delay that a retry policy or processor code asks for is
 * shortened to it, so that the attempt still ends.
 */
final class StoredTime {
    static final Duration MAX = TaskRequest.MAX_TIME_FROM_NOW;

    private StoredTime() {}

    /** @return true when the duration is null or at most {@link #MAX} */
    static boolean fits(Duration duration) {
        return duration == null || duration.compareTo(MAX) <= 0;
    }

    /** @return true when the instant is null or within {@link #MAX} of {@code now} */
    static boolean fits(Instant instant, Instant now) {
        return instant == null || Duration.between(now, instant).abs().compareTo(MAX) <= 0;
    }

    /** @throws TaskValidationException when the duration is longer than {@link #MAX} */
    static void require(String field, Duration duration) {
        if (!fits(duration)) throw new TaskValidationException(field + " must be at most " + MAX);
    }

    /** @throws TaskValidationException when the instant is further than {@link #MAX} from {@code now} */
    static void require(String field, Instant instant, Instant now) {
        if (!fits(instant, now)) throw new TaskValidationException(field + " must be within " + MAX + " of now");
    }

    /** @return {@code now} plus the delay, a delay longer than {@link #MAX} counting as {@link #MAX} */
    static Instant after(Instant now, Duration delay) {
        return now.plus(fits(delay) ? delay : MAX);
    }
}
