package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

class ScheduleDefinitionTest {

    private static final TaskType<String> WITH_PAYLOAD = TaskType.of("report", String.class);
    private static final TaskType<Void> NO_PAYLOAD = TaskType.of("cleanup");

    @Test
    void minimumFixedDelayIsOneSecond() {
        assertThat(ScheduleDefinition.MIN_FIXED_DELAY).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void cronFactoryLeavesFixedDelayAndOptionsUnset() {
        var d = ScheduleDefinition.of("nightly", WITH_PAYLOAD, "data", "0 2 * * *");

        assertThat(d.id()).isEqualTo("nightly");
        assertThat(d.type()).isEqualTo(WITH_PAYLOAD);
        assertThat(d.payload()).isEqualTo("data");
        assertThat(d.cron()).isEqualTo("0 2 * * *");
        assertThat(d.fixedDelay()).isNull();
        assertThat(d.isFixedDelay()).isFalse();
        assertThat(d.zone()).isNull();
        assertThat(d.queue()).isNull();
        assertThat(d.account()).isNull();
        assertThat(d.maxAttempts()).isNull();
    }

    @Test
    void payloadlessCronFactory() {
        var d = ScheduleDefinition.of("nightly", NO_PAYLOAD, "0 2 * * *");

        assertThat(d.payload()).isNull();
        assertThat(d.cron()).isEqualTo("0 2 * * *");
    }

    @Test
    void everyFactoryCreatesAFixedDelayDefinition() {
        var d = ScheduleDefinition.every("poll", WITH_PAYLOAD, "data", Duration.ofMinutes(5));

        assertThat(d.cron()).isNull();
        assertThat(d.fixedDelay()).isEqualTo(Duration.ofMinutes(5));
        assertThat(d.isFixedDelay()).isTrue();
        assertThat(d.payload()).isEqualTo("data");
    }

    @Test
    void payloadlessEveryFactory() {
        var d = ScheduleDefinition.every("poll", NO_PAYLOAD, Duration.ofSeconds(1));

        assertThat(d.payload()).isNull();
        assertThat(d.fixedDelay()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void misfireDefaultsToFireOnceNowAndOverlapToAllow() {
        var d = ScheduleDefinition.of("nightly", NO_PAYLOAD, "0 2 * * *");

        assertThat(d.misfire()).isEqualTo(Misfire.FIRE_ONCE_NOW);
        assertThat(d.overlap()).isEqualTo(Overlap.ALLOW);
    }

    @Test
    void nullIdIsRejected() {
        assertThatThrownBy(() -> ScheduleDefinition.of(null, NO_PAYLOAD, "0 2 * * *"))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("schedule id");
    }

    @Test
    void blankIdIsRejected() {
        assertThatThrownBy(() -> ScheduleDefinition.of("", NO_PAYLOAD, "0 2 * * *"))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.of("   ", NO_PAYLOAD, "0 2 * * *"))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void idLongerThan128CharactersIsRejectedAndExactly128IsAccepted() {
        assertThat(ScheduleDefinition.of("a".repeat(128), NO_PAYLOAD, "0 2 * * *")
                        .id())
                .hasSize(128);
        assertThatThrownBy(() -> ScheduleDefinition.of("a".repeat(129), NO_PAYLOAD, "0 2 * * *"))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void nullTypeIsRejected() {
        assertThatThrownBy(() -> ScheduleDefinition.of("id", (TaskType<Void>) null, "0 2 * * *"))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("type");
    }

    @Test
    void fixedDelayBelowTheMinimumIsRejected() {
        assertThatThrownBy(() -> ScheduleDefinition.every("id", NO_PAYLOAD, Duration.ofMillis(999)))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("fixedDelay");
        assertThatThrownBy(() -> ScheduleDefinition.every("id", NO_PAYLOAD, Duration.ZERO))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> ScheduleDefinition.every("id", NO_PAYLOAD, Duration.ofSeconds(-5)))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void fixedDelayOfExactlyTheMinimumIsAccepted() {
        assertThat(ScheduleDefinition.every("id", NO_PAYLOAD, ScheduleDefinition.MIN_FIXED_DELAY)
                        .fixedDelay())
                .isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void neitherCronNorFixedDelayIsRejected() {
        assertThatThrownBy(() -> ScheduleDefinition.of("id", NO_PAYLOAD, null))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(() -> ScheduleDefinition.of("id", NO_PAYLOAD, "  "))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void bothCronAndFixedDelayAreRejected() {
        assertThatThrownBy(() -> new ScheduleDefinition<>(
                        "id", NO_PAYLOAD, null, "0 2 * * *", Duration.ofMinutes(1), null, null, null, null, null, null))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void typeWithPayloadRequiresOne() {
        assertThatThrownBy(() -> ScheduleDefinition.of("id", WITH_PAYLOAD, null, "0 2 * * *"))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void maxAttemptsOutsideOneToShortMaxIsRejected() {
        var d = ScheduleDefinition.of("id", NO_PAYLOAD, "0 2 * * *");

        assertThatThrownBy(() -> d.maxAttempts(0)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> d.maxAttempts(-1)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> d.maxAttempts(Short.MAX_VALUE + 1)).isInstanceOf(TaskValidationException.class);
        assertThat(d.maxAttempts(1).maxAttempts()).isEqualTo(1);
        assertThat(d.maxAttempts(Short.MAX_VALUE).maxAttempts()).isEqualTo((int) Short.MAX_VALUE);
    }

    @Test
    void withersChangeOnlyTheirFieldAndReturnNewInstances() {
        var base = ScheduleDefinition.of("id", WITH_PAYLOAD, "data", "0 2 * * *");
        var berlin = ZoneId.of("Europe/Berlin");

        assertThat(base.zone(berlin)).isNotSameAs(base).isEqualTo(copyWith(base, berlin, null, null, null, null, null));
        assertThat(base.misfire(Misfire.SKIP)).isEqualTo(copyWith(base, null, Misfire.SKIP, null, null, null, null));
        assertThat(base.overlap(Overlap.SKIP)).isEqualTo(copyWith(base, null, null, Overlap.SKIP, null, null, null));
        assertThat(base.queue("Q1")).isEqualTo(copyWith(base, null, null, null, "Q1", null, null));
        assertThat(base.account("acme")).isEqualTo(copyWith(base, null, null, null, null, "acme", null));
        assertThat(base.maxAttempts(3)).isEqualTo(copyWith(base, null, null, null, null, null, 3));
        assertThat(base.zone()).isNull();
        assertThat(base.queue()).isNull();
    }

    @Test
    void withersKeepEarlierSettings() {
        var d = ScheduleDefinition.every("id", WITH_PAYLOAD, "data", Duration.ofMinutes(2))
                .zone(ZoneId.of("UTC"))
                .misfire(Misfire.catchUp(4))
                .overlap(Overlap.SKIP)
                .queue("Q1")
                .account("acme")
                .maxAttempts(7);

        assertThat(d.id()).isEqualTo("id");
        assertThat(d.type()).isEqualTo(WITH_PAYLOAD);
        assertThat(d.payload()).isEqualTo("data");
        assertThat(d.fixedDelay()).isEqualTo(Duration.ofMinutes(2));
        assertThat(d.zone()).isEqualTo(ZoneId.of("UTC"));
        assertThat(d.misfire()).isEqualTo(new Misfire.CatchUp(4));
        assertThat(d.overlap()).isEqualTo(Overlap.SKIP);
        assertThat(d.queue()).isEqualTo("Q1");
        assertThat(d.account()).isEqualTo("acme");
        assertThat(d.maxAttempts()).isEqualTo(7);
    }

    @Test
    void settingMisfireOrOverlapToNullRestoresTheDefault() {
        var d = ScheduleDefinition.of("id", NO_PAYLOAD, "0 2 * * *")
                .misfire(Misfire.SKIP)
                .overlap(Overlap.SKIP)
                .misfire(null)
                .overlap(null);

        assertThat(d.misfire()).isEqualTo(Misfire.FIRE_ONCE_NOW);
        assertThat(d.overlap()).isEqualTo(Overlap.ALLOW);
    }

    @Test
    void misfireConstantsAndFactory() {
        assertThat(Misfire.SKIP).isInstanceOf(Misfire.Skip.class);
        assertThat(Misfire.FIRE_ONCE_NOW).isInstanceOf(Misfire.FireOnceNow.class);
        assertThat(Misfire.catchUp(5)).isEqualTo(new Misfire.CatchUp(5));
    }

    @Test
    void catchUpWithMaxBelowOneIsRejected() {
        assertThatThrownBy(() -> Misfire.catchUp(0)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> new Misfire.CatchUp(-3)).isInstanceOf(TaskConfigurationError.class);
        assertThat(Misfire.catchUp(1)).isEqualTo(new Misfire.CatchUp(1));
    }

    private static ScheduleDefinition<String> copyWith(
            ScheduleDefinition<String> base,
            ZoneId zone,
            Misfire misfire,
            Overlap overlap,
            String queue,
            String account,
            Integer maxAttempts) {
        return new ScheduleDefinition<>(
                base.id(),
                base.type(),
                base.payload(),
                base.cron(),
                base.fixedDelay(),
                zone != null ? zone : base.zone(),
                misfire != null ? misfire : base.misfire(),
                overlap != null ? overlap : base.overlap(),
                queue != null ? queue : base.queue(),
                account != null ? account : base.account(),
                maxAttempts != null ? maxAttempts : base.maxAttempts());
    }
}
