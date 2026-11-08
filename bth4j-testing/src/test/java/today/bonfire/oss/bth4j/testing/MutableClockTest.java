package today.bonfire.oss.bth4j.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class MutableClockTest {

    @Test
    void aZonedViewHasTheZoneAndFollowsTheClock() {
        var clock = new MutableClock(Instant.parse("2030-01-01T00:00:00Z"));
        var berlin = ZoneId.of("Europe/Berlin");

        var zoned = clock.withZone(berlin);
        clock.advance(Duration.ofMinutes(5));

        assertThat(zoned.getZone()).isEqualTo(berlin);
        assertThat(zoned.instant()).isEqualTo(Instant.parse("2030-01-01T00:05:00Z"));
        assertThat(zoned.withZone(ZoneId.of("UTC")).instant()).isEqualTo(clock.instant());
    }
}
