package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

class TaskRequestTest {

    record Payload(String name) {}

    private static final TaskType<Payload> TYPED = TaskType.of("typed", Payload.class);
    private static final TaskType<Void> VOID = TaskType.of("void");

    @Test
    void typedTypeRequiresAPayload() {
        assertThatThrownBy(() -> TaskRequest.of(TYPED, null))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("needs a payload");
    }

    @Test
    void typeIsRequired() {
        assertThatThrownBy(() -> TaskRequest.of((TaskType<Payload>) null, new Payload("x")))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> TaskRequest.of((TaskType<Void>) null)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void voidTypeNeedsNoPayload() {
        var request = TaskRequest.of(VOID).build();

        assertThat(request.payload()).isNull();
        assertThat(request.type()).isEqualTo(VOID);
    }

    @Test
    void defaultsAreEmpty() {
        var request = TaskRequest.of(TYPED, new Payload("x")).build();

        assertThat(request.queue()).isNull();
        assertThat(request.account()).isNull();
        assertThat(request.at()).isNull();
        assertThat(request.delay()).isNull();
        assertThat(request.uniqueKey()).isNull();
        assertThat(request.uniqueness()).isEqualTo(Uniqueness.UNTIL_DONE);
        assertThat(request.maxAttempts()).isNull();
        assertThat(request.headers()).isEmpty();
        assertThat(request.id()).isNull();
        assertThat(request.payload()).isEqualTo(new Payload("x"));
    }

    @Test
    void negativeDelayIsRejected() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.delay(Duration.ofMillis(-1))).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void zeroAndNullDelayAreAccepted() {
        assertThat(TaskRequest.of(VOID).delay(Duration.ZERO).build().delay()).isEqualTo(Duration.ZERO);
        assertThat(TaskRequest.of(VOID).delay(null).build().delay()).isNull();
    }

    @Test
    void atAndDelayReplaceEachOther() {
        var at = Instant.parse("2031-01-01T00:00:00Z");

        var delayed = TaskRequest.of(VOID).at(at).delay(Duration.ofSeconds(5)).build();
        var timed = TaskRequest.of(VOID).delay(Duration.ofSeconds(5)).at(at).build();

        assertThat(delayed.at()).isNull();
        assertThat(delayed.delay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(timed.delay()).isNull();
        assertThat(timed.at()).isEqualTo(at);
    }

    @Test
    void uniqueKeyLengthBoundary() {
        var longest = "k".repeat(TaskRequest.MAX_UNIQUE_KEY_LENGTH);
        var builder = TaskRequest.of(VOID);

        assertThat(TaskRequest.of(VOID).uniqueKey(longest).build().uniqueKey()).isEqualTo(longest);
        assertThatThrownBy(() -> builder.uniqueKey(longest + "k")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.uniqueKey("")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.uniqueKey("  ")).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void aUniqueKeyMayNotStartLikeTheKeyOfAScheduleOccurrence() {
        assertThatThrownBy(() -> TaskRequest.of(VOID).uniqueKey("sched:nightly"))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("sched:");
        assertThatThrownBy(() -> TaskRequest.of(VOID).uniqueKey("sched:", Uniqueness.keepFor(Duration.ofMinutes(1))))
                .isInstanceOf(TaskValidationException.class);
        assertThat(TaskRequest.of(VOID).uniqueKey("schedule-7").build().uniqueKey())
                .isEqualTo("schedule-7");
    }

    @Test
    void aUniqueKeySetAgainWithoutAUniquenessDropsTheEarlierKeepWindow() {
        var request = TaskRequest.of(VOID)
                .uniqueKey("k", Uniqueness.keepFor(Duration.ofMinutes(1)))
                .uniqueKey("other")
                .build();

        assertThat(request.uniqueKey()).isEqualTo("other");
        assertThat(request.uniqueness()).isEqualTo(Uniqueness.UNTIL_DONE);
    }

    @Test
    void uniqueKeyWithUniquenessKeepsBoth() {
        var request = TaskRequest.of(VOID)
                .uniqueKey("k", Uniqueness.keepFor(Duration.ofMinutes(1)))
                .build();

        assertThat(request.uniqueKey()).isEqualTo("k");
        assertThat(request.uniqueness()).isEqualTo(new Uniqueness.KeepFor(Duration.ofMinutes(1)));
    }

    @Test
    void nullUniquenessIsRejected() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.uniqueKey("k", null)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void maxAttemptsBounds() {
        var builder = TaskRequest.of(VOID);

        assertThat(TaskRequest.of(VOID).maxAttempts(1).build().maxAttempts()).isEqualTo(1);
        assertThat(TaskRequest.of(VOID)
                        .maxAttempts(TaskRequest.MAX_ATTEMPTS)
                        .build()
                        .maxAttempts())
                .isEqualTo((int) Short.MAX_VALUE);
        assertThatThrownBy(() -> builder.maxAttempts(0)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.maxAttempts(-3)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.maxAttempts(TaskRequest.MAX_ATTEMPTS + 1))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void headersAreCollectedAndImmutable() {
        var request = TaskRequest.of(VOID)
                .addHeader("a", "1")
                .addHeaders(Map.of("b", "2"))
                .addHeader("a", "3")
                .build();

        assertThat(request.headers()).containsOnly(Map.entry("a", "3"), Map.entry("b", "2"));
        assertThatThrownBy(() -> request.headers().put("c", "4")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void headerNameMustNotBeBlankAndValueMustNotBeNull() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.addHeader(null, "v")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.addHeader(" ", "v")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.addHeader("n", null)).isInstanceOf(TaskValidationException.class);
        assertThat(TaskRequest.of(VOID).addHeader("n", "").build().headers()).containsEntry("n", "");
    }

    @Test
    void nullHeaderMapIsIgnored() {
        assertThat(TaskRequest.of(VOID).addHeaders(null).build().headers()).isEmpty();
    }

    @Test
    void textWithAU0000IsRejected() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.account("a\u0000b")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.uniqueKey("k\u0000")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.addHeader("n\u0000", "v")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.addHeader("n", "v\u0000")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.retryPolicy("p\u0000")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.retryPolicy(" ")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.retryPolicy("p".repeat(TaskRequest.MAX_RETRY_POLICY_LENGTH + 1)))
                .isInstanceOf(TaskValidationException.class);
        assertThatCode(() -> builder.retryPolicy("p".repeat(TaskRequest.MAX_RETRY_POLICY_LENGTH)))
                .doesNotThrowAnyException();
        assertThatCode(() -> builder.retryPolicy(null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> TaskRequest.requireUniqueKey("k\u0000")).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void aDelayIsAtMostOneHundredYears() {
        assertThat(TaskRequest.MAX_TIME_FROM_NOW).isEqualTo(Duration.ofDays(36_525));
        assertThat(TaskRequest.of(VOID)
                        .delay(TaskRequest.MAX_TIME_FROM_NOW)
                        .build()
                        .delay())
                .isEqualTo(TaskRequest.MAX_TIME_FROM_NOW);
        assertThatThrownBy(() -> TaskRequest.of(VOID).delay(TaskRequest.MAX_TIME_FROM_NOW.plusMillis(1)))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("delay");
    }

    @Test
    void keepForIsPositiveAndAtMostOneHundredYears() {
        assertThat(Uniqueness.MAX_KEEP_FOR).isEqualTo(TaskRequest.MAX_TIME_FROM_NOW);
        assertThat(Uniqueness.keepFor(Uniqueness.MAX_KEEP_FOR))
                .isEqualTo(new Uniqueness.KeepFor(Duration.ofDays(36_525)));
        assertThatThrownBy(() -> Uniqueness.keepFor(Uniqueness.MAX_KEEP_FOR.plusMillis(1)))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> Uniqueness.keepFor(Duration.ZERO)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void accountLengthBoundary() {
        var ok = "a".repeat(TaskRequest.MAX_ACCOUNT_LENGTH);
        var builder = TaskRequest.of(VOID);

        assertThat(TaskRequest.of(VOID).account(ok).build().account()).isEqualTo(ok);
        assertThatThrownBy(() -> builder.account(ok + "a")).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void maxRunTimeMustBeAtLeastOneMillisecondAndAtMostMaxRunTime() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.maxRunTime(Duration.ZERO)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.maxRunTime(Duration.ofSeconds(-1)))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.maxRunTime(Duration.ofNanos(999_999)))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("maxRunTime must be at least 1 ms and at most " + TaskRequest.MAX_RUN_TIME);
        assertThat(TaskRequest.of(VOID).maxRunTime(Duration.ofMillis(1)).build().maxRunTime())
                .isEqualTo(Duration.ofMillis(1));
        assertThatThrownBy(() -> builder.maxRunTime(TaskRequest.MAX_RUN_TIME.plusMillis(1)))
                .isInstanceOf(TaskValidationException.class);
        for (var unbounded : new Duration[] {ChronoUnit.FOREVER.getDuration(), Duration.ofSeconds(Long.MIN_VALUE)}) {
            assertThatThrownBy(() -> builder.maxRunTime(unbounded))
                    .as("a duration that has no millisecond value")
                    .isInstanceOf(TaskValidationException.class);
        }
        assertThat(TaskRequest.of(VOID)
                        .maxRunTime(TaskRequest.MAX_RUN_TIME)
                        .build()
                        .maxRunTime())
                .isEqualTo(TaskRequest.MAX_RUN_TIME);
        assertThat(TaskRequest.of(VOID)
                        .maxRunTime(Duration.ofSeconds(1))
                        .build()
                        .maxRunTime())
                .isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void idMustBePositive() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.id(0)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.id(-5)).isInstanceOf(TaskValidationException.class);
        assertThat(TaskRequest.of(VOID).id(42).build().id()).isEqualTo(42L);
    }

    @Test
    void withIdPreservesEveryOtherFieldAndTheOriginalRequest() {
        var request = TaskRequest.of(TYPED, new Payload("copy"))
                .queue("other")
                .account("account")
                .at(Instant.parse("2031-01-01T00:00:00Z"))
                .uniqueKey("key", Uniqueness.keepFor(Duration.ofMinutes(5)))
                .maxAttempts(7)
                .addHeader("a", "b")
                .startBy(Instant.parse("2031-01-02T00:00:00Z"))
                .maxRunTime(Duration.ofSeconds(3))
                .retryPolicy("policy")
                .build();

        var copy = request.withId(42);

        assertThat(copy).usingRecursiveComparison().ignoringFields("id").isEqualTo(request);
        assertThat(copy.id()).isEqualTo(42L);
        assertThat(copy.payload()).isSameAs(request.payload());
        assertThat(request.id()).isNull();
        assertThat(copy.withId(43).id()).isEqualTo(43L);
        assertThat(copy.id()).isEqualTo(42L);
        var delayed = TaskRequest.of(VOID).delay(Duration.ofMinutes(1)).build();
        assertThat(delayed.withId(44))
                .usingRecursiveComparison()
                .ignoringFields("id")
                .isEqualTo(delayed);
        assertThatThrownBy(() -> request.withId(0)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> request.withId(-1)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void uniquenessKeepForMustBeAtLeastOneMillisecond() {
        assertThatThrownBy(() -> Uniqueness.keepFor(Duration.ZERO)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> Uniqueness.keepFor(Duration.ofNanos(999_999)))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("duration must be at least 1 ms");
        assertThat(Uniqueness.keepFor(Duration.ofMillis(1))).isEqualTo(new Uniqueness.KeepFor(Duration.ofMillis(1)));
        assertThatThrownBy(() -> Uniqueness.keepFor(Duration.ofSeconds(-1)))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> Uniqueness.keepFor(null)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> Uniqueness.keepFor(ChronoUnit.FOREVER.getDuration()))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("duration must be at most " + Uniqueness.MAX_KEEP_FOR);
        assertThatThrownBy(() -> Uniqueness.keepFor(Duration.ofSeconds(Long.MIN_VALUE)))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("duration must be at least 1 ms");
    }

    @Test
    void aUniqueKeyRuleRefusesNull() {
        assertThatThrownBy(() -> TaskRequest.requireUniqueKey(null)).isInstanceOf(TaskValidationException.class);
    }
}
