package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/** The promotion loop against an in-memory store whose clock stands years behind the wall clock. */
@Timeout(10)
class ScheduledTaskServiceTest {
    private final MutableClock clock = new MutableClock(Instant.parse("2020-01-01T00:00:00Z"));
    private final InMemoryStore store = new InMemoryStore(clock);
    private final RunnerStatus status = new RunnerStatus(clock);

    private ScheduledTaskService service(FullStore serviceStore, Duration pollInterval) {
        return new ScheduledTaskService.Builder()
                .setGroup(new ThreadGroup("promotion-test"))
                .setThreadName("promotion-test")
                .setStore(serviceStore)
                .setClock(clock)
                .setStatus(status)
                .setPollInterval(pollInterval)
                .create();
    }

    @Test
    void theWaitForTheNextDueTaskIsMeasuredOnTheRunnerClock() {
        var poll = Duration.ofSeconds(5);
        var service = service(store, poll);
        var now = clock.instant();

        assertThat(service.sleepTime(null)).isEqualTo(poll);
        assertThat(service.sleepTime(now.plus(Duration.ofHours(1)))).isEqualTo(poll);
        assertThat(service.sleepTime(now.plusMillis(200))).isEqualTo(Duration.ofMillis(200));
        assertThat(service.sleepTime(now.minusSeconds(1))).isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void aStoreErrorInPromotionIsReportedAsAnIssueAndTheLoopGoesOn() {
        var running = new AtomicReference<ScheduledTaskService>();
        var promotes = new AtomicInteger();
        var failing = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("promote")) {
                if (promotes.incrementAndGet() == 1) throw new TaskStoreException("store down");
                running.get().stopThread();
            }
            return real.proceed();
        });
        var service = service(failing, Duration.ofMillis(1));
        running.set(service);

        service.run();

        assertThat(promotes).hasValue(2);
        assertThat(status.issues()).extracting(Issue::kind).containsExactly("store-error");
    }
}
