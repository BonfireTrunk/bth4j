package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
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
        assertThat(TaskRequest.of(VOID).maxAttempts(Short.MAX_VALUE).build().maxAttempts())
                .isEqualTo((int) Short.MAX_VALUE);
        assertThatThrownBy(() -> builder.maxAttempts(0)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.maxAttempts(-3)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.maxAttempts(Short.MAX_VALUE + 1)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void headersAreCollectedAndImmutable() {
        var request = TaskRequest.of(VOID)
                .header("a", "1")
                .headers(Map.of("b", "2"))
                .header("a", "3")
                .build();

        assertThat(request.headers()).containsOnly(Map.entry("a", "3"), Map.entry("b", "2"));
        assertThatThrownBy(() -> request.headers().put("c", "4")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void headerNameMustNotBeBlankAndValueMustNotBeNull() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.header(null, "v")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.header(" ", "v")).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.header("n", null)).isInstanceOf(TaskValidationException.class);
        assertThat(TaskRequest.of(VOID).header("n", "").build().headers()).containsEntry("n", "");
    }

    @Test
    void nullHeaderMapIsIgnored() {
        assertThat(TaskRequest.of(VOID).headers(null).build().headers()).isEmpty();
    }

    @Test
    void accountLengthBoundary() {
        var ok = "a".repeat(TaskRequest.MAX_ACCOUNT_LENGTH);
        var builder = TaskRequest.of(VOID);

        assertThat(TaskRequest.of(VOID).account(ok).build().account()).isEqualTo(ok);
        assertThatThrownBy(() -> builder.account(ok + "a")).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void maxRunMustBePositive() {
        var builder = TaskRequest.of(VOID);

        assertThatThrownBy(() -> builder.maxRun(Duration.ZERO)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> builder.maxRun(Duration.ofSeconds(-1))).isInstanceOf(TaskValidationException.class);
        assertThat(TaskRequest.of(VOID).maxRun(Duration.ofSeconds(1)).build().maxRun())
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
    void uniquenessKeepForMustBePositive() {
        assertThatThrownBy(() -> Uniqueness.keepFor(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Uniqueness.keepFor(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Uniqueness.keepFor(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
