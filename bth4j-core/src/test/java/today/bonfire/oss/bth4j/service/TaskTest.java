package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.store.TaskRecord;

class TaskTest {

    private static final Function<String, Event> PARSER = v -> "5".equals(v) ? TestEvents.RETRY_TASK : null;

    @Test
    void emptyAccountIdIsRejected() {
        assertThatThrownBy(() ->
                        Task.Builder.newTask().event(TestEvents.REGULAR_TASK).build())
                .isInstanceOf(TaskErrorException.class);
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .event(TestEvents.REGULAR_TASK)
                        .accountId("")
                        .build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void nullEventIsRejected() {
        assertThatThrownBy(() -> Task.Builder.newTask().accountId(1).build()).isInstanceOf(TaskErrorException.class);
    }

    @Test
    void recurringWithoutCronAndWithoutExecutionTimeIsRejected() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .event(TestEvents.RECURRING)
                        .accountId(1)
                        .build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void recurringWithExecutionTimeButNoCronIsAccepted() {
        assertThatCode(() -> Task.Builder.newTask()
                        .event(TestEvents.RECURRING)
                        .accountId(1)
                        .executeAfter(Duration.ofMinutes(1))
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    void nonRecurringEventWithCronIsRejected() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .event(TestEvents.REGULAR_TASK)
                        .accountId(1)
                        .cronExpression("* * * * *")
                        .build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void invalidCronIsRejectedWithFormatHint() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .event(TestEvents.RECURRING)
                        .accountId(1)
                        .cronExpression("not a cron")
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("5 field");
    }

    @Test
    void sixFieldCronIsRejected() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .event(TestEvents.RECURRING)
                        .accountId(1)
                        .cronExpression("0 * * * * *")
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("5 field");
    }

    @Test
    void newTaskGeneratesIdAndKeepsGivenId() {
        var generated = Task.Builder.newTask()
                .event(TestEvents.REGULAR_TASK)
                .accountId(1)
                .build();
        var given = Task.Builder.newTask("x")
                .event(TestEvents.REGULAR_TASK)
                .accountId(1)
                .build();

        assertThat(generated.uniqueId()).isNotBlank();
        assertThat(given.uniqueId()).isEqualTo("x");
    }

    @Test
    void numericAccountIdBecomesString() {
        var task = Task.Builder.newTask()
                .event(TestEvents.REGULAR_TASK)
                .accountId(42)
                .build();

        assertThat(task.accountId()).isEqualTo("42");
    }

    @Test
    void maxAttemptsIsRetryCountPlusOne() {
        var regular = Task.Builder.newTask()
                .event(TestEvents.REGULAR_TASK)
                .accountId(1)
                .build();
        var retry =
                Task.Builder.newTask().event(TestEvents.RETRY_TASK).accountId(1).build();

        assertThat(regular.maxAttempts()).isEqualTo(1);
        assertThat(retry.maxAttempts()).isEqualTo(TestEvents.RETRY_TASK.retryCount() + 1);
        assertThat(retry.attempts()).isZero();
    }

    @Test
    void executeAfterSetsFutureTime() {
        var task = Task.Builder.newTask()
                .event(TestEvents.REGULAR_TASK)
                .accountId(1)
                .executeAfter(Duration.ofMinutes(5))
                .build();

        assertThat(task.isExecutionTimeSet()).isTrue();
        assertThat(task.executeAtTimeStamp()).isAfter(Instant.now());
    }

    @Test
    void fromRecordParsesEveryField() {
        var record = new TaskRecord(
                "id-1",
                "5",
                "42",
                "hi",
                "{\"a\":1}",
                2,
                4,
                Instant.now(),
                Instant.ofEpochMilli(1700000000000L),
                "tok",
                null,
                "sched-1",
                "boom",
                null);

        var task = Task.from(record, PARSER);

        assertThat(task.uniqueId()).isEqualTo("id-1");
        assertThat(task.event()).isEqualTo(TestEvents.RETRY_TASK);
        assertThat(task.accountId()).isEqualTo("42");
        assertThat(task.queueName()).isEqualTo("hi");
        assertThat(task.executeAtTimeStamp()).isEqualTo(Instant.ofEpochMilli(1700000000000L));
        assertThat(task.attempts()).isEqualTo(2);
        assertThat(task.maxAttempts()).isEqualTo(4);
        assertThat(task.scheduleId()).isEqualTo("sched-1");
        assertThat(task.data()).isEqualTo("{\"a\":1}");
        assertThat(task.lastError()).isEqualTo("boom");
        assertThat(task.token()).isEqualTo("tok");
    }

    @Test
    void fromRecordWithOnlyRequiredFieldsUsesDefaults() {
        var task = Task.from(TaskRecord.newTask("id", "5", "1", null, null, 0, null), PARSER);

        assertThat(task.attempts()).isZero();
        assertThat(task.maxAttempts()).isEqualTo(TestEvents.RETRY_TASK.retryCount() + 1);
        assertThat(task.executeAtTimeStamp()).isNull();
        assertThat(task.data()).isNull();
        assertThat(task.token()).isNull();
    }

    @Test
    void fromRecordWithUnknownEventThrows() {
        assertThatThrownBy(() -> Task.from(TaskRecord.newTask("id", "999", "1", null, null, 0, null), PARSER))
                .isInstanceOf(TaskErrorException.class);
        assertThatThrownBy(() -> Task.from(TaskRecord.newTask("id", null, "1", null, null, 0, null), PARSER))
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void nextExecutionIsNextFiveMinuteBoundary() {
        var next = Task.nextExecution("*/5 * * * *", Instant.parse("2024-01-01T10:02:30Z"));

        assertThat(next).contains(Instant.parse("2024-01-01T10:05:00Z"));
    }

    @Test
    void nextExecutionIsStrictlyAfterABoundary() {
        var next = Task.nextExecution("*/5 * * * *", Instant.parse("2024-01-01T10:05:00Z"));

        assertThat(next).contains(Instant.parse("2024-01-01T10:10:00Z"));
    }

    @Test
    void toStringContainsId() {
        var task = Task.Builder.newTask("the-id")
                .event(TestEvents.REGULAR_TASK)
                .accountId(1)
                .build();

        assertThat(task.toString()).contains("the-id");
    }
}
