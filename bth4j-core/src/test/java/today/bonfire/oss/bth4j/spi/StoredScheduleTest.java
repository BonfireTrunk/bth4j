package today.bonfire.oss.bth4j.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class StoredScheduleTest {

    private static StoredSchedule schedule(Duration fixedDelay, Duration maxRunTime) {
        return new StoredSchedule(
                "s", 1, 1, 1, null, null, null, null, 0, fixedDelay, false, null, 3, maxRunTime, false, 0);
    }

    @Test
    void durationsAreCutToWholeMilliseconds() {
        var s = schedule(Duration.ofNanos(90_000_999_999L), Duration.ofNanos(5_000_000_001L));

        assertThat(s.fixedDelay()).isEqualTo(Duration.ofSeconds(90));
        assertThat(s.maxRunTime()).isEqualTo(Duration.ofSeconds(5));
        assertThat(s).isEqualTo(schedule(Duration.ofSeconds(90), Duration.ofSeconds(5)));
    }

    @Test
    void absentDurationsStayAbsent() {
        var s = schedule(null, null);

        assertThat(s.fixedDelay()).isNull();
        assertThat(s.maxRunTime()).isNull();
        assertThat(s.isFixedDelay()).isFalse();
    }

    @Test
    void theCopiesKeepEveryOtherField() {
        var s = schedule(Duration.ofSeconds(90), Duration.ofSeconds(5));

        assertThat(s.withRevision(7))
                .isEqualTo(new StoredSchedule(
                        "s",
                        1,
                        1,
                        1,
                        null,
                        null,
                        null,
                        null,
                        0,
                        Duration.ofSeconds(90),
                        false,
                        null,
                        3,
                        Duration.ofSeconds(5),
                        false,
                        7));
        assertThat(s.withPaused(true).paused()).isTrue();
        assertThat(s.withPaused(true).withPaused(false)).isEqualTo(s);
    }
}
