package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

class OutageLogTest {
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final AtomicLong nanos = new AtomicLong(5_000_000_000L);
    private Logger logger;
    private OutageLog outage;

    @BeforeEach
    void attach() {
        logger = (Logger) LoggerFactory.getLogger("today.bonfire.oss.bth4j.OutageLogTest");
        logger.setLevel(Level.DEBUG);
        logs.start();
        logger.addAppender(logs);
        outage = new OutageLog(logger, "Test loop", nanos::get);
    }

    @AfterEach
    void detach() {
        logger.detachAppender(logs);
    }

    private ILoggingEvent line(int index) {
        return logs.list.get(index);
    }

    @Test
    void theFirstFailureIsLoggedAsTheLoopLoggedItBeforeWithTheStackTrace() {
        var e = new IllegalStateException("down");

        outage.failed(e, "Round of {} failed", 7);

        assertThat(logs.list).hasSize(1);
        assertThat(line(0).getLevel()).isEqualTo(Level.ERROR);
        assertThat(line(0).getFormattedMessage()).isEqualTo("Round of 7 failed");
        assertThat(line(0).getThrowableProxy().getMessage()).isEqualTo("down");
    }

    @Test
    void aLaterFailureOfTheSameKindIsOneLineWithTheCountTheClassAndTheMessage() {
        outage.failed(new IllegalStateException("down"), "Round of {} failed", 7);
        outage.failed(new IllegalStateException("still down"), "Round of {} failed", 7);
        outage.failed(new IllegalStateException("still down"), "Round of {} failed", 7);

        assertThat(logs.list).hasSize(3);
        assertThat(line(1).getLevel()).isEqualTo(Level.ERROR);
        assertThat(line(1).getThrowableProxy()).isNull();
        assertThat(line(1).getFormattedMessage())
                .isEqualTo("Round of 7 failed (failure 2 in a row: java.lang.IllegalStateException: still down)");
        assertThat(line(2).getThrowableProxy()).isNull();
        assertThat(line(2).getFormattedMessage()).contains("failure 3 in a row");
    }

    @Test
    void aFailureOfAnotherExceptionClassBringsTheStackTraceBack() {
        outage.failed(new IllegalStateException("a"), "Round failed");
        outage.failed(new IllegalArgumentException("b"), "Round failed");

        assertThat(line(1).getThrowableProxy()).isNotNull();
        assertThat(line(1).getFormattedMessage()).isEqualTo("Round failed");
    }

    @Test
    void theSameClassWithAnotherRootCauseClassBringsTheStackTraceBack() {
        outage.failed(new RuntimeException("a", new IOException("refused")), "Round failed");
        outage.failed(new RuntimeException("a", new IllegalStateException("timeout")), "Round failed");

        assertThat(line(1).getThrowableProxy()).isNotNull();
    }

    @Test
    void anotherExceptionClassWithTheSameRootCauseClassBringsTheStackTraceBack() {
        outage.failed(new RuntimeException("a", new IOException("refused")), "Round failed");
        outage.failed(new IllegalStateException("a", new IOException("refused")), "Round failed");

        assertThat(line(1).getThrowableProxy()).isNotNull();
    }

    @Test
    void anotherKindBecomesTheKindToCompareWith() {
        outage.failed(new IllegalStateException("a"), "Round failed");
        outage.failed(new IllegalArgumentException("b"), "Round failed");
        outage.failed(new IllegalArgumentException("c"), "Round failed");

        assertThat(line(2).getThrowableProxy()).isNull();
        assertThat(line(2).getFormattedMessage()).contains("failure 3 in a row");
    }

    @Test
    void theSameClassAndRootCauseWithAnotherMessageIsStillTheSameKind() {
        outage.failed(new RuntimeException("connect to 10.0.0.1:5432", new IOException("refused 1")), "Round failed");
        outage.failed(new RuntimeException("connect to 10.0.0.2:5432", new IOException("refused 2")), "Round failed");

        assertThat(line(1).getThrowableProxy()).isNull();
    }

    @Test
    void aSuccessAfterFailuresLogsTheRecoveryOnceWithTheCountAndTheSeconds() {
        outage.failed(new IllegalStateException("a"), "Round failed");
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2500));
        outage.failed(new IllegalStateException("a"), "Round failed");
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(3700));

        outage.succeeded();
        outage.succeeded();

        assertThat(logs.list).hasSize(3);
        assertThat(line(2).getLevel()).isEqualTo(Level.INFO);
        assertThat(line(2).getThrowableProxy()).isNull();
        assertThat(line(2).getFormattedMessage())
                .isEqualTo("Test loop: the store answers again after 2 failed attempts over 6.2 seconds");
    }

    @Test
    void aSuccessWithoutFailuresLogsNothing() {
        outage.succeeded();
        outage.succeeded();

        assertThat(logs.list).isEmpty();
    }

    @Test
    void afterARecoveryTheNextFailureIsAFirstFailureAgain() {
        outage.failed(new IllegalStateException("a"), "Round failed");
        outage.failed(new IllegalStateException("a"), "Round failed");
        outage.succeeded();

        outage.failed(new IllegalStateException("a"), "Round failed");
        outage.failed(new IllegalStateException("a"), "Round failed");

        assertThat(logs.list).hasSize(5);
        assertThat(line(3).getThrowableProxy()).isNotNull();
        assertThat(line(3).getFormattedMessage()).isEqualTo("Round failed");
        assertThat(line(4).getFormattedMessage()).contains("failure 2 in a row");
        outage.succeeded();
        assertThat(line(5).getFormattedMessage()).contains("after 2 failed attempts");
    }

    @Test
    @Timeout(10)
    void aCauseCycleDoesNotHang() {
        var a = new RuntimeException("a");
        var b = new RuntimeException("b", a);
        a.initCause(b);

        outage.failed(a, "Round failed");
        outage.failed(a, "Round failed");

        assertThat(logs.list).hasSize(2);
        assertThat(line(1).getThrowableProxy()).isNull();
    }

    @Test
    @Timeout(10)
    void aThrowableThatIsItsOwnCauseDoesNotHang() {
        var e = new RuntimeException("self") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        outage.failed(e, "Round failed");
        outage.failed(e, "Round failed");

        assertThat(logs.list).hasSize(2);
    }
}
