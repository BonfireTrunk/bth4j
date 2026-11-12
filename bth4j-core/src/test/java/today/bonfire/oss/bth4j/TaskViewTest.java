package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TaskViewTest {

    private static final Instant T = Instant.parse("2030-01-01T00:00:00Z");

    @Test
    void aViewBuiltByHandHoldsEveryFieldItWasGiven() {
        var failure = new FailureInfo(2, T.plusSeconds(8), "java.io.IOException", "reset", null);
        var step = new StepResult(T.plusSeconds(9), "\"done\"");

        var view = TaskView.builder()
                .id(41)
                .revision(3)
                .queue("mail")
                .type("send")
                .payloadVersion(2)
                .state(TaskState.DEAD)
                .attempts(4)
                .maxAttempts(5)
                .createdAt(T)
                .due(T.plusSeconds(1))
                .stateChangedAt(T.plusSeconds(2))
                .encodedPayload("{}")
                .runnerId(7)
                .leaseUntil(T.plusSeconds(3))
                .claimedAt(T.plusSeconds(4))
                .account("acme")
                .headers(Map.of("trace", "abc"))
                .uniqueKey("invoice-9")
                .keyKeep(Duration.ofHours(1))
                .keyUntil(T.plusSeconds(5))
                .retryPolicy("slow")
                .scheduleId("nightly")
                .retryOf(40L)
                .startBy(T.plusSeconds(6))
                .maxRunTime(Duration.ofMinutes(2))
                .cancelRequestedAt(T.plusSeconds(7))
                .cancelReason("operator")
                .progress("half")
                .steps(Map.of("charge", step))
                .failures(List.of(failure))
                .build();

        assertThat(view.id()).isEqualTo(41);
        assertThat(view.revision()).isEqualTo(3);
        assertThat(view.queue()).isEqualTo("mail");
        assertThat(view.type()).isEqualTo("send");
        assertThat(view.payloadVersion()).isEqualTo(2);
        assertThat(view.state()).isEqualTo(TaskState.DEAD);
        assertThat(view.attempts()).isEqualTo(4);
        assertThat(view.maxAttempts()).isEqualTo(5);
        assertThat(view.createdAt()).isEqualTo(T);
        assertThat(view.due()).isEqualTo(T.plusSeconds(1));
        assertThat(view.stateChangedAt()).isEqualTo(T.plusSeconds(2));
        assertThat(view.encodedPayload()).isEqualTo("{}");
        assertThat(view.runnerId()).isEqualTo(7);
        assertThat(view.leaseUntil()).isEqualTo(T.plusSeconds(3));
        assertThat(view.claimedAt()).isEqualTo(T.plusSeconds(4));
        assertThat(view.account()).isEqualTo("acme");
        assertThat(view.headers()).containsExactly(Map.entry("trace", "abc"));
        assertThat(view.uniqueKey()).isEqualTo("invoice-9");
        assertThat(view.keyKeep()).isEqualTo(Duration.ofHours(1));
        assertThat(view.keyUntil()).isEqualTo(T.plusSeconds(5));
        assertThat(view.retryPolicy()).isEqualTo("slow");
        assertThat(view.scheduleId()).isEqualTo("nightly");
        assertThat(view.retryOf()).isEqualTo(40L);
        assertThat(view.startBy()).isEqualTo(T.plusSeconds(6));
        assertThat(view.maxRunTime()).isEqualTo(Duration.ofMinutes(2));
        assertThat(view.cancelRequestedAt()).isEqualTo(T.plusSeconds(7));
        assertThat(view.cancelReason()).isEqualTo("operator");
        assertThat(view.progress()).isEqualTo("half");
        assertThat(view.steps()).containsExactly(Map.entry("charge", step));
        assertThat(view.failures()).containsExactly(failure);
        assertThat(view.lastFailure()).isEqualTo(failure);
        assertThat(view).hasToString("Task[#41, send, queue=mail, DEAD, attempts=4/5]");
    }

    @Test
    void aViewBuiltWithOnlyItsNamesHasEmptyFields() {
        var view = TaskView.builder().queue("mail").type("send").build();

        assertThat(view.id()).isZero();
        assertThat(view.state()).isNull();
        assertThat(view.runnerId()).isNull();
        assertThat(view.headers()).isEmpty();
        assertThat(view.steps()).isEmpty();
        assertThat(view.failures()).isEmpty();
        assertThat(view.lastFailure()).isNull();
    }
}
