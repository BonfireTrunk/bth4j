package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.exceptions.RedactedFailure;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

class LoggingTaskObserverTest {

    private static final String SENTINEL = "customer-4711@example.invalid";

    @Test
    void retryOfARedactedFailureLogsTheClassNameOnly() {
        var logger = (Logger) LoggerFactory.getLogger("bth4j.tasks.test");
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        try {
            var record = TaskRecord.builder()
                    .id(5)
                    .queueId(1)
                    .eventId(1)
                    .state(TaskState.SCHEDULED)
                    .attempts(1)
                    .maxAttempts(3)
                    .build();
            var task = new TaskView(record, "Q:DEF", "boom");
            var error = new RedactedFailure(IllegalStateException.class.getName());

            new LoggingTaskObserver(logger)
                    .onEvent(new TaskEvent.Retrying(
                            task, error, Instant.parse("2026-06-01T12:00:00Z"), TaskEvent.Outcome.APPLIED));

            assertThat(logs.list).hasSize(1);
            var event = logs.list.get(0);
            assertThat(event.getFormattedMessage())
                    .endsWith(": " + IllegalStateException.class.getName())
                    .doesNotContain(SENTINEL);
            assertThat(event.getThrowableProxy()).isNull();
        } finally {
            logger.detachAppender(logs);
        }
    }
}
