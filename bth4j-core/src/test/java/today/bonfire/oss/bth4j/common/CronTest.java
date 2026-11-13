package today.bonfire.oss.bth4j.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
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
    void nextRejectsAnExpressionThatDoesNotParseAsValidateDoes() {
        assertThatThrownBy(() -> Cron.next("not a cron", Instant.parse("2026-01-01T00:00:00Z")))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("not a cron");
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

    @Test
    void aGapBehindARepeatedHourIsStillFoundWhenItIsTheFirstFireAfterIt() {
        // from April 2025 the first transition is the October overlap; 02:30 on 29 March does not exist in 2026
        var fire = Cron.next("30 2 29 3 *", Instant.parse("2025-04-01T00:00:00Z"), BERLIN)
                .orElseThrow();

        assertThat(fire).isEqualTo(Instant.parse("2026-03-29T01:00:00Z"));
    }

    @Test
    void aGapIsFoundWhenAnOverlapAndAnotherYearsGapLieBetweenAndTheMatch() {
        var fire = Cron.next("30 2 30 3 *", Instant.parse("2024-04-01T00:00:00Z"), BERLIN)
                .orElseThrow();

        assertThat(fire).isEqualTo(Instant.parse("2025-03-30T01:00:00Z"));
    }

    @Test
    void anExpressionWithoutAMatchInAGapIsNotMovedByAnEarlierOverlap() {
        var fire = Cron.next("30 3 29 3 *", Instant.parse("2025-04-01T00:00:00Z"), BERLIN)
                .orElseThrow();

        assertThat(fire).isEqualTo(Instant.parse("2026-03-29T01:30:00Z"));
    }

    @Test
    void aMatchLaterOnTheDayTheClocksGoForwardByHalfAnHourIsNotMovedToTheNextDay() {
        // Lord Howe Island: 02:00 becomes 02:30 on 6 October 2019, and `after` is 01:30 that night
        var fire = Cron.next("30 2 * * *", Instant.parse("2019-10-05T15:00:00Z"), ZoneId.of("Australia/Lord_Howe"));

        assertThat(fire).contains(Instant.parse("2019-10-05T15:30:00Z"));
    }

    @Test
    void aMatchInAGapOfThreeQuartersOfAnHourFiresWhenThatGapEnds() {
        // Chatham Islands: 02:45 becomes 03:45 on 29 September 2019
        var fire = Cron.next("0 3 * * *", Instant.parse("2019-09-28T12:00:00Z"), ZoneId.of("Pacific/Chatham"));

        assertThat(fire).contains(Instant.parse("2019-09-28T14:00:00Z"));
    }

    @Test
    void aYearlyExpressionAskedOnADayWhoseMidnightIsSkippedFiresInTheComingYear() {
        // Santiago: midnight becomes 01:00 on 8 September 2019, and `after` is that moment
        var fire = Cron.next("0 0 1 1 *", Instant.parse("2019-09-08T04:00:00Z"), ZoneId.of("America/Santiago"));

        assertThat(fire).contains(Instant.parse("2020-01-01T03:00:00Z"));
    }

    @Test
    void aFixedTimeAskedForInsideItsRepeatedHourDoesNotFireInTheSecondPass() {
        var newYork = ZoneId.of("America/New_York");
        // 01:00:03 in the first pass of the hour that 3 November 2019 repeats; the fire of 01:00 has just happened
        var fromTheFirstPass = Cron.next("0 1 * * *", Instant.parse("2019-11-03T05:00:03Z"), newYork);
        var fromTheSecondPass = Cron.next("30 1 * * *", Instant.parse("2019-11-03T06:10:00Z"), newYork);

        assertThat(fromTheFirstPass).contains(Instant.parse("2019-11-04T06:00:00Z"));
        assertThat(fromTheSecondPass).contains(Instant.parse("2019-11-04T06:30:00Z"));
    }

    @Test
    void aListOfFixedHoursFiresOncePerListedHourWhenOneOfThemRepeats() {
        // Berlin repeats 02:00 to 03:00 on 25 October 2026
        assertThat(fires("0 1,2 * * *", "2026-10-24T22:00:00Z", BERLIN, 3))
                .containsExactly(
                        Instant.parse("2026-10-24T23:00:00Z"),
                        Instant.parse("2026-10-25T00:00:00Z"),
                        Instant.parse("2026-10-26T00:00:00Z"));
    }

    @Test
    void anExpressionWithAWildcardMinuteOrHourKeepsItsRhythmThroughARepeatedHour() {
        assertThat(fires("0 * * * *", "2026-10-24T23:30:00Z", BERLIN, 4))
                .containsExactly(
                        Instant.parse("2026-10-25T00:00:00Z"),
                        Instant.parse("2026-10-25T01:00:00Z"),
                        Instant.parse("2026-10-25T02:00:00Z"),
                        Instant.parse("2026-10-25T03:00:00Z"));
        assertThat(fires("*/30 2 * * *", "2026-10-24T23:30:00Z", BERLIN, 5))
                .containsExactly(
                        Instant.parse("2026-10-25T00:00:00Z"),
                        Instant.parse("2026-10-25T00:30:00Z"),
                        Instant.parse("2026-10-25T01:00:00Z"),
                        Instant.parse("2026-10-25T01:30:00Z"),
                        Instant.parse("2026-10-26T01:00:00Z"));
    }

    @Test
    void severalMatchesInOneSkippedHourFireOnceTogether() {
        assertThat(fires("*/15 2 * * *", "2026-03-28T12:00:00Z", BERLIN, 2))
                .containsExactly(Instant.parse("2026-03-29T01:00:00Z"), Instant.parse("2026-03-30T00:00:00Z"));
    }

    @Test
    void fireTimesAroundEveryClockChangeAreThoseOfAPlainSearchThroughTheWallClock() {
        var expressions = List.of(
                "*/5 * * * *",
                "0 2 * * *",
                "30 2 * * *",
                "0 1 * * *",
                "30 1 * * *",
                "0 0 * * *",
                "59 23 31 * *",
                "0 0 29 2 *",
                "15 14 1 * *",
                "0 */2 * * *",
                "*/30 * * * *",
                "0 3 * * 1",
                "0 0 * * 0",
                "0 12 * * 1-5",
                "5,35 1-3 * * *",
                "0 0 1 1 *",
                "45 2 * * *",
                "0 2-3 * * *",
                "*/15 1-2 * * *",
                "30 0 * * *",
                "0 23 * * *",
                "10 0 * * *",
                "0 */6 * * *",
                "0 0,1,2,3 * * *",
                "*/20 * * * *");
        var zones = List.of(
                "America/New_York",
                "Europe/London",
                "Europe/Berlin",
                "Australia/Lord_Howe",
                "Asia/Kolkata",
                "Pacific/Apia",
                "Pacific/Chatham",
                "America/Sao_Paulo",
                "Africa/Casablanca",
                "America/St_Johns",
                "Antarctica/Troll",
                "Asia/Tehran",
                "America/Havana",
                "Europe/Dublin",
                "Asia/Gaza",
                "America/Santiago");
        var wrong = new ArrayList<String>();
        int compared = 0;
        for (var id : zones) {
            var zone = ZoneId.of(id);
            var starts = new ArrayList<Instant>();
            var change = zone.getRules().nextTransition(Instant.parse("2010-06-01T00:00:00Z"));
            for (int n = 0; n < 8 && change != null; n++) {
                for (long minutes = -720; minutes <= 720; minutes += 45) {
                    starts.add(change.getInstant().plus(Duration.ofMinutes(minutes)));
                }
                starts.add(change.getInstant().plusSeconds(3));
                change = zone.getRules().nextTransition(change.getInstant());
            }
            starts.add(Instant.parse("2019-01-15T10:20:30Z"));
            for (var expression : expressions) {
                for (var start : starts) {
                    compared++;
                    var got = Cron.next(expression, start, zone).orElse(null);
                    var want = plainSearch(expression, start, zone);
                    if (!want.equals(got) && wrong.size() < 10) {
                        wrong.add(expression + " after " + start + " in " + id + ": " + got + ", not " + want);
                    }
                }
            }
        }

        assertThat(wrong).isEmpty();
        assertThat(compared).isGreaterThan(50_000);
    }

    private static List<Instant> fires(String expression, String after, ZoneId zone, int count) {
        var fires = new ArrayList<Instant>();
        var t = Instant.parse(after);
        for (int i = 0; i < count; i++) {
            t = Cron.next(expression, t, zone).orElseThrow();
            fires.add(t);
        }
        return fires;
    }

    /**
     * The fire time by its definition, without the library that parses the expressions: every matching wall-clock
     * minute of every day is placed by the zone's rules, and the earliest one after {@code after} wins.
     */
    private static Instant plainSearch(String expression, Instant after, ZoneId zone) {
        var f = expression.split(" ");
        var minutes = field(f[0], 0, 59);
        var hours = field(f[1], 0, 23);
        var daysOfMonth = field(f[2], 1, 31);
        var months = field(f[3], 1, 12);
        var daysOfWeek = field(f[4], 0, 6);
        boolean everyMinuteOrHour = f[0].startsWith("*") || f[1].startsWith("*");
        var rules = zone.getRules();
        var first = after.atZone(zone).toLocalDate().minusDays(2);
        Instant best = null;
        for (int d = 0; d < 366 * 9; d++) {
            LocalDate day = first.plusDays(d);
            if (best != null && day.isAfter(best.atZone(zone).toLocalDate().plusDays(2))) break;
            if (!months.get(day.getMonthValue())
                    || !daysOfMonth.get(day.getDayOfMonth())
                    || !daysOfWeek.get(day.getDayOfWeek().getValue() % 7)) {
                continue;
            }
            for (int h = hours.nextSetBit(0); h >= 0; h = hours.nextSetBit(h + 1)) {
                for (int m = minutes.nextSetBit(0); m >= 0; m = minutes.nextSetBit(m + 1)) {
                    var local = day.atTime(h, m);
                    var offsets = rules.getValidOffsets(local);
                    var placed = new ArrayList<Instant>();
                    if (offsets.isEmpty()) placed.add(rules.getTransition(local).getInstant());
                    else placed.add(local.toInstant(offsets.get(0)));
                    if (offsets.size() == 2 && everyMinuteOrHour) placed.add(local.toInstant(offsets.get(1)));
                    for (var instant : placed) {
                        if (instant.isAfter(after) && (best == null || instant.isBefore(best))) best = instant;
                    }
                }
            }
        }
        return best;
    }

    private static BitSet field(String field, int lowest, int highest) {
        var values = new BitSet();
        for (var part : field.split(",")) {
            var rangeAndStep = part.split("/");
            int step = rangeAndStep.length == 2 ? Integer.parseInt(rangeAndStep[1]) : 1;
            int from = lowest;
            int to = highest;
            if (!rangeAndStep[0].equals("*")) {
                var range = rangeAndStep[0].split("-");
                from = Integer.parseInt(range[0]);
                to = range.length == 2 ? Integer.parseInt(range[1]) : from;
            }
            for (int v = from; v <= to; v += step) values.set(v);
        }
        return values;
    }

    @Test
    void theCacheOfParsedExpressionsIsBoundedAndSpacingDoesNotMakeAnotherEntry() {
        var after = Instant.parse("2026-01-01T00:00:00Z");
        var before = Cron.cached();
        Cron.next("  0   5 * * * ", after);
        Cron.next("0 5 * * *", after);
        assertThat(Cron.cached()).isLessThanOrEqualTo(before + 1);

        for (int minute = 0; minute < 60; minute++) {
            for (int hour = 0; hour < 24; hour++) {
                Cron.next(minute + " " + hour + " * * *", after);
            }
        }

        assertThat(Cron.cached()).isLessThanOrEqualTo(256);
    }
}
