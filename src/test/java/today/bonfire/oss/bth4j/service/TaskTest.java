package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;

class TaskTest {

    private static final Function<String, Event> PARSER = TestEvents.UNKNOWN::from;

    /** event with a configurable value, used to check validation of the event value */
    private record FixedEvent(String value, boolean recurring) implements Event {
        @Override
        public boolean isRecurring() {
            return recurring;
        }

        @Override
        public int retryCount() {
            return 0;
        }

        @Override
        public Event from(String v) {
            return new FixedEvent(v, recurring);
        }
    }

    // build() validation

    @Test
    void buildRequiresAccountId() {
        assertThatThrownBy(() ->
                        Task.Builder.newTask().event(TestEvents.REGULAR_TASK).build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void buildRequiresEvent() {
        assertThatThrownBy(() -> Task.Builder.newTask().accountId("acct").build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void newTaskKeepsCallerSuppliedId() {
        var task = Task.Builder.newTask("my-id")
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        assertThat(task.uniqueId()).isEqualTo("my-id");
        assertThat(task.taskString()).startsWith("my-id|");
    }

    @Test
    void newTaskGeneratesIdWhenNullOrBlank() {
        var a = Task.Builder.newTask()
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        var b = Task.Builder.newTask(null)
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        var c = Task.Builder.newTask("  ")
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        assertThat(a.uniqueId()).isNotBlank();
        assertThat(b.uniqueId()).isNotBlank().isNotEqualTo(a.uniqueId());
        assertThat(c.uniqueId()).isNotBlank().isNotEqualTo(b.uniqueId());
    }

    @Test
    void separatorIsRejectedInUniqueId() {
        assertThatThrownBy(() -> Task.Builder.newTask("a|b")
                        .accountId("acct")
                        .event(TestEvents.REGULAR_TASK)
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("uniqueId");
    }

    @Test
    void separatorIsRejectedInAccountId() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("x|y")
                        .event(TestEvents.REGULAR_TASK)
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("accountId");
    }

    @Test
    void separatorIsRejectedInAccountIdThatIsNotAString() {
        var accountId = new Object() {
            @Override
            public String toString() {
                return "a|b";
            }
        };
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId(accountId)
                        .event(TestEvents.REGULAR_TASK)
                        .build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void separatorIsRejectedInQueueName() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("acct")
                        .queueName("Q|1")
                        .event(TestEvents.REGULAR_TASK)
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("queueName");
    }

    @Test
    void separatorIsRejectedInEventValue() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("acct")
                        .event(new FixedEvent("a|b", false))
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("event value");
    }

    @Test
    void separatorIsRejectedInCronExpression() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("acct")
                        .event(TestEvents.RECURRING_TASK)
                        .cronExpression("*/5 * * * *|x")
                        .build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void recurringTaskRequiresCronExpression() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("acct")
                        .event(TestEvents.RECURRING_TASK)
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("cron");
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("acct")
                        .event(TestEvents.RECURRING_TASK)
                        .cronExpression(" ")
                        .build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void recurringTaskRejectsSixFieldCronExpression() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("acct")
                        .event(TestEvents.RECURRING_TASK)
                        .cronExpression("0 */15 * * * *")
                        .build())
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("cron");
    }

    @Test
    void recurringTaskRejectsGarbageCronExpression() {
        assertThatThrownBy(() -> Task.Builder.newTask()
                        .accountId("acct")
                        .event(TestEvents.RECURRING_TASK)
                        .cronExpression("not a cron")
                        .build())
                .isInstanceOf(TaskErrorException.class);
    }

    @Test
    void recurringEventWithExecutionTimeDoesNotNeedCronExpression() {
        // this is a single run of a recurring task, queued by the recurring task service
        var task = Task.Builder.newTask("id1")
                .accountId("acct")
                .event(TestEvents.RECURRING_TASK)
                .executeAfter(10)
                .build();
        assertThat(task.cronExpression()).isNull();
        assertThat(task.isExecutionTimeSet()).isTrue();
        assertThat(task.taskString()).isEqualTo("id1|200|acct");
    }

    @Test
    void cronExpressionIsIgnoredForNonRecurringEvents() {
        var task = Task.Builder.newTask("id1")
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .cronExpression("not a cron")
                .build();
        assertThat(task.taskString()).isEqualTo("id1|3|acct");
    }

    // task string and round trip

    @Test
    void regularTaskStringHasThreeParts() {
        var task = Task.Builder.newTask("id1")
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        assertThat(task.taskString()).isEqualTo("id1|3|acct");
        assertThat(task.toString()).isEqualTo(task.taskString());
        assertThat(task.isNULL()).isFalse();
        assertThat(task.isExecutionTimeSet()).isFalse();
    }

    @Test
    void regularTaskRoundTrip() {
        var task = Task.Builder.newTask("id1")
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        var parsed = new Task(task.taskString(), PARSER);

        assertThat(parsed.uniqueId()).isEqualTo("id1");
        assertThat(parsed.event()).isEqualTo(TestEvents.REGULAR_TASK);
        assertThat(parsed.accountId()).isEqualTo("acct");
        assertThat(parsed.queueName()).isNull();
        assertThat(parsed.cronExpression()).isNull();
        assertThat(parsed.taskString()).isEqualTo(task.taskString());
    }

    @Test
    void queuedTaskRoundTrip() {
        var task = Task.Builder.newTask("id1")
                .accountId(42)
                .queueName("Q:LOW")
                .event(TestEvents.REGULAR_TASK)
                .build();
        assertThat(task.taskString()).isEqualTo("id1|3|42|Q:LOW");

        var parsed = new Task(task.taskString(), PARSER);
        assertThat(parsed.queueName()).isEqualTo("Q:LOW");
        assertThat(parsed.accountId()).isEqualTo("42");
        assertThat(parsed.buildTaskString()).isEqualTo(task.taskString());
    }

    @Test
    void blankQueueNameInTaskStringIsParsedAsNull() {
        var parsed = new Task("id1|3|acct|", PARSER);
        assertThat(parsed.queueName()).isNull();
    }

    @Test
    void recurringTaskRoundTrip() {
        var task = Task.Builder.newTask("id1")
                .accountId("acct")
                .queueName("Q:MED")
                .event(TestEvents.RECURRING_TASK)
                .cronExpression("*/15 * * * *")
                .build();
        assertThat(task.taskString()).isEqualTo("id1|200|acct|Q:MED|*/15 * * * *");

        var parsed = new Task(task.taskString(), PARSER);
        assertThat(parsed.event()).isEqualTo(TestEvents.RECURRING_TASK);
        assertThat(parsed.queueName()).isEqualTo("Q:MED");
        assertThat(parsed.cronExpression()).isEqualTo("*/15 * * * *");
    }

    @Test
    void recurringTaskWithoutQueueKeepsPositionOfCronExpression() {
        var task = Task.Builder.newTask("id1")
                .accountId("acct")
                .event(TestEvents.RECURRING_TASK)
                .cronExpression("0 22 * * 1-5")
                .build();
        assertThat(task.taskString()).isEqualTo("id1|200|acct||0 22 * * 1-5");

        var parsed = new Task(task.taskString(), PARSER);
        assertThat(parsed.queueName()).isNull();
        assertThat(parsed.cronExpression()).isEqualTo("0 22 * * 1-5");
    }

    // parsing

    @Test
    void blankTaskStringGivesNullTask() {
        assertThat(new Task(null, PARSER).isNULL()).isTrue();
        assertThat(new Task("", PARSER).isNULL()).isTrue();
        assertThat(new Task("   ", PARSER).isNULL()).isTrue();
    }

    @Test
    void malformedTaskStringIsRejected() {
        assertThatThrownBy(() -> new Task("id1|3", PARSER)).isInstanceOf(TaskErrorException.class);
        assertThatThrownBy(() -> new Task("a|3|c|d|e|f", PARSER)).isInstanceOf(TaskErrorException.class);
    }

    @Test
    void eventParserReturningNullIsRejected() {
        assertThatThrownBy(() -> new Task("id1|99|acct", s -> null))
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("Event parser returned null");
    }

    // execution time

    @Test
    void nextExecutionTimeFollowsCronExpression() {
        var task = Task.Builder.newTask("id1")
                .accountId("acct")
                .event(TestEvents.RECURRING_TASK)
                .cronExpression("*/15 * * * *")
                .build();

        assertThat(task.getNextExecutionTime(Instant.parse("2024-01-01T00:07:00Z")))
                .isEqualTo(Instant.parse("2024-01-01T00:15:00Z"));
        // the time passed in is excluded, so the next run is the following slot
        assertThat(task.getNextExecutionTime(Instant.parse("2024-01-01T00:15:00Z")))
                .isEqualTo(Instant.parse("2024-01-01T00:30:00Z"));
        assertThat(task.getNextExecutionTime(Instant.parse("2024-01-01T23:50:00Z")))
                .isEqualTo(Instant.parse("2024-01-02T00:00:00Z"));
    }

    @Test
    void executeAfterSetsExecutionTimeInTheFuture() {
        var before = Instant.now();
        var task = Task.Builder.newTask()
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .executeAfter(60)
                .build();
        assertThat(task.isExecutionTimeSet()).isTrue();
        assertThat(task.executeAtTimeStamp()).isAfter(before.plusSeconds(59));
    }

    // data

    @Test
    void getDataReturnsDataOfTheRightType() {
        var task = Task.Builder.newTask()
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        task.setData("payload");
        assertThat(task.getData(String.class)).isEqualTo("payload");
        assertThat(task.data()).isEqualTo("payload");
    }

    @Test
    void getDataReturnsNullWhenNoDataIsSet() {
        var task = Task.Builder.newTask()
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        assertThat(task.getData(String.class)).isNull();
    }

    @Test
    void getDataWithWrongTypeThrowsTaskDataException() {
        var task = Task.Builder.newTask()
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
        task.setData("payload");
        assertThatThrownBy(() -> task.getData(Integer.class))
                .isInstanceOf(TaskDataException.class)
                .hasMessageContaining("java.lang.Integer")
                .hasMessageContaining("java.lang.String");
    }
}
