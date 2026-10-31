package today.bonfire.oss.bth4j.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

class CronTest {

    private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");

    private static Instant next(String expression, String after) {
        return Cron.next(expression, Instant.parse(after)).orElseThrow();
    }

    @Test
    void nextIsStrictlyAfterTheGivenInstant() {
        assertThat(next("*/5 * * * *", "2026-01-01T10:05:00Z")).isEqualTo(Instant.parse("2026-01-01T10:10:00Z"));
        assertThat(next("*/5 * * * *", "2026-01-01T10:04:59Z")).isEqualTo(Instant.parse("2026-01-01T10:05:00Z"));
    }

    @Test
    void nextWithoutZoneEvaluatesInUtc() {
        var after = Instant.parse("2026-01-01T00:00:00Z");

        assertThat(Cron.next("30 8 * * *", after)).isEqualTo(Cron.next("30 8 * * *", after, ZoneOffset.UTC));
        assertThat(Cron.next("30 8 * * *", after)).contains(Instant.parse("2026-01-01T08:30:00Z"));
    }

    @Test
    void nullZoneFallsBackToUtc() {
        var after = Instant.parse("2026-01-01T00:00:00Z");

        assertThat(Cron.next("30 8 * * *", after, null)).contains(Instant.parse("2026-01-01T08:30:00Z"));
    }

    @Test
    void zoneShiftsTheFireTimeByItsOffset() {
        var after = Instant.parse("2026-01-01T00:00:00Z");

        assertThat(Cron.next("0 9 * * *", after, BERLIN)).contains(Instant.parse("2026-01-01T08:00:00Z"));
        assertThat(Cron.next("0 9 * * *", after, ZoneId.of("America/New_York")))
                .contains(Instant.parse("2026-01-01T14:00:00Z"));
    }

    @Test
    void summerAndWinterOffsetsBothApplyForTheSameWallClockTime() {
        assertThat(Cron.next("0 9 * * *", Instant.parse("2026-07-01T00:00:00Z"), BERLIN))
                .contains(Instant.parse("2026-07-01T07:00:00Z"));
        assertThat(Cron.next("0 9 * * *", Instant.parse("2026-12-01T00:00:00Z"), BERLIN))
                .contains(Instant.parse("2026-12-01T08:00:00Z"));
    }

    @Test
    void wallClockTimeSkippedBySpringForwardFiresWhenTheGapEnds() {
        var fire = Cron.next("0 2 * * *", Instant.parse("2026-03-28T12:00:00Z"), BERLIN)
                .orElseThrow();

        assertThat(fire).isEqualTo(Instant.parse("2026-03-29T01:00:00Z"));
        assertThat(fire.atZone(BERLIN).toLocalTime()).hasToString("03:00");

        var following = Cron.next("0 2 * * *", fire, BERLIN).orElseThrow();
        assertThat(following).isEqualTo(Instant.parse("2026-03-30T00:00:00Z"));
    }

    @Test
    void halfHourInsideTheSkippedHourAlsoFiresWhenTheGapEnds() {
        var fire = Cron.next("30 2 * * *", Instant.parse("2026-03-28T12:00:00Z"), BERLIN)
                .orElseThrow();

        assertThat(fire).isEqualTo(Instant.parse("2026-03-29T01:00:00Z"));
    }

    @Test
    void expressionOutsideTheSkippedHourIsUnaffectedBySpringForward() {
        var fire = Cron.next("30 3 * * *", Instant.parse("2026-03-28T12:00:00Z"), BERLIN)
                .orElseThrow();

        assertThat(fire).isEqualTo(Instant.parse("2026-03-29T01:30:00Z"));
    }

    @Test
    void wallClockTimeRepeatedByFallBackFiresOnlyOncePerDay() {
        var first = Cron.next("0 2 * * *", Instant.parse("2026-10-24T12:00:00Z"), BERLIN)
                .orElseThrow();
        var second = Cron.next("0 2 * * *", first, BERLIN).orElseThrow();

        assertThat(first.atZone(BERLIN).toLocalDate()).hasToString("2026-10-25");
        assertThat(second.atZone(BERLIN).toLocalDate()).hasToString("2026-10-26");
        assertThat(second.atZone(BERLIN).toLocalTime()).hasToString("02:00");
    }

    @Test
    void hourlyExpressionNeverRepeatsAnInstantAcrossFallBack() {
        var t = Instant.parse("2026-10-25T00:00:00Z");
        var fires = new ArrayList<Instant>();
        for (int i = 0; i < 4; i++) {
            t = Cron.next("0 * * * *", t, BERLIN).orElseThrow();
            fires.add(t);
        }

        assertThat(fires).doesNotHaveDuplicates().isSorted();
    }

    @Test
    void commaListFiresOnEachListedValue() {
        assertThat(next("0,15,45 * * * *", "2026-01-01T10:00:00Z")).isEqualTo(Instant.parse("2026-01-01T10:15:00Z"));
        assertThat(next("0,15,45 * * * *", "2026-01-01T10:15:00Z")).isEqualTo(Instant.parse("2026-01-01T10:45:00Z"));
        assertThat(next("0,15,45 * * * *", "2026-01-01T10:45:00Z")).isEqualTo(Instant.parse("2026-01-01T11:00:00Z"));
    }

    @Test
    void rangeFiresOnlyInsideTheRange() {
        assertThat(next("0 9-11 * * *", "2026-01-01T08:00:00Z")).isEqualTo(Instant.parse("2026-01-01T09:00:00Z"));
        assertThat(next("0 9-11 * * *", "2026-01-01T11:00:00Z")).isEqualTo(Instant.parse("2026-01-02T09:00:00Z"));
    }

    @Test
    void stepOverWildcardAndOverRange() {
        assertThat(next("*/20 * * * *", "2026-01-01T10:00:00Z")).isEqualTo(Instant.parse("2026-01-01T10:20:00Z"));
        assertThat(next("10-30/10 * * * *", "2026-01-01T10:10:00Z")).isEqualTo(Instant.parse("2026-01-01T10:20:00Z"));
        assertThat(next("10-30/10 * * * *", "2026-01-01T10:30:00Z")).isEqualTo(Instant.parse("2026-01-01T11:10:00Z"));
    }

    @Test
    void monthAndWeekdayNamesAreAccepted() {
        assertThat(next("0 0 1 jan *", "2026-02-01T00:00:00Z")).isEqualTo(Instant.parse("2027-01-01T00:00:00Z"));
        assertThat(next("0 12 * * mon", "2026-01-01T00:00:00Z")).isEqualTo(Instant.parse("2026-01-05T12:00:00Z"));
    }

    @Test
    void weekdayRangeSkipsTheWeekend() {
        assertThat(next("0 12 * * 1-5", "2026-01-02T12:00:00Z")).isEqualTo(Instant.parse("2026-01-05T12:00:00Z"));
    }

    @Test
    void expressionThatNeverMatchesReturnsEmpty() {
        assertThat(Cron.next("0 0 30 2 *", Instant.parse("2026-01-01T00:00:00Z")))
                .isEmpty();
    }

    @Test
    void validateAcceptsFiveFieldExpressions() {
        Cron.validate("*/5 0-6 1,15 * mon-fri");
    }

    @Test
    void validateRejectsTooFewFields() {
        assertThatThrownBy(() -> Cron.validate("* * * *"))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("* * * *");
    }

    @Test
    void validateRejectsSixFieldExpressions() {
        assertThatThrownBy(() -> Cron.validate("0 0 0 * * *")).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void validateRejectsOutOfRangeValuesAndGarbage() {
        assertThatThrownBy(() -> Cron.validate("61 * * * *")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> Cron.validate("not a cron")).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void validateRejectsNullAndBlank() {
        assertThatThrownBy(() -> Cron.validate(null)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> Cron.validate("  ")).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void zoneOfAValidIdIsThatZone() {
        assertThat(Cron.zone("Europe/Berlin")).isEqualTo(BERLIN);
        assertThat(Cron.zone("UTC")).isEqualTo(ZoneId.of("UTC"));
    }

    @Test
    void zoneOfNullIsUtc() {
        assertThat(Cron.zone(null)).isEqualTo(ZoneOffset.UTC);
    }

    @Test
    void zoneOfAnUnknownIdIsRejected() {
        assertThatThrownBy(() -> Cron.zone("Mars/Olympus"))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("Mars/Olympus");
    }

    @Test
    void zoneOfABlankIdIsRejectedRatherThanDefaulted() {
        assertThatThrownBy(() -> Cron.zone("")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> Cron.zone("  ")).isInstanceOf(TaskValidationException.class);
    }
}
