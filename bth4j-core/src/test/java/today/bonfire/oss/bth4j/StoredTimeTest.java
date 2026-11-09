package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

class StoredTimeTest {

    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Test
    void anInstantFitsWithinOneHundredYearsEitherWay() {
        assertThat(StoredTime.fits(null, NOW)).isTrue();
        assertThat(StoredTime.fits(NOW.plus(StoredTime.MAX), NOW)).isTrue();
        assertThat(StoredTime.fits(NOW.minus(StoredTime.MAX), NOW)).isTrue();
        assertThat(StoredTime.fits(NOW.plus(StoredTime.MAX).plusMillis(1), NOW)).isFalse();
        assertThat(StoredTime.fits(NOW.minus(StoredTime.MAX).minusMillis(1), NOW))
                .isFalse();
        assertThat(StoredTime.fits(Instant.MAX, NOW)).isFalse();
        assertThat(StoredTime.fits(Instant.MIN, NOW)).isFalse();
    }

    @Test
    void aDurationFitsUpToOneHundredYears() {
        assertThat(StoredTime.fits(null)).isTrue();
        assertThat(StoredTime.fits(StoredTime.MAX)).isTrue();
        assertThat(StoredTime.fits(StoredTime.MAX.plusMillis(1))).isFalse();
        assertThat(StoredTime.fits(ChronoUnit.FOREVER.getDuration())).isFalse();
    }

    @Test
    void requireNamesTheField() {
        assertThatThrownBy(() -> StoredTime.require("delay", StoredTime.MAX.plusMillis(1)))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageStartingWith("delay ");
        assertThatThrownBy(() -> StoredTime.require("at", Instant.MAX, NOW))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageStartingWith("at ");
        StoredTime.require("delay", StoredTime.MAX);
        StoredTime.require("at", NOW.minus(StoredTime.MAX), NOW);
    }

    @Test
    void afterShortensALongerDelayToOneHundredYears() {
        assertThat(StoredTime.after(NOW, Duration.ofSeconds(5))).isEqualTo(NOW.plusSeconds(5));
        assertThat(StoredTime.after(NOW, StoredTime.MAX)).isEqualTo(NOW.plus(StoredTime.MAX));
        assertThat(StoredTime.after(NOW, ChronoUnit.FOREVER.getDuration())).isEqualTo(NOW.plus(StoredTime.MAX));
    }
}
