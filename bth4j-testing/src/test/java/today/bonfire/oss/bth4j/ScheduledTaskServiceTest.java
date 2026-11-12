package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.PromoteResult;
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
    void aTaskThatIsDueByThisClockButNotMovedIsAskedAboutLessOftenEachRoundUpToThePollInterval() {
        var poll = Duration.ofMillis(10);
        var service = service(store, poll);
        var overdue = clock.instant().minusSeconds(2);

        assertThat(List.of(
                        service.sleepTime(overdue),
                        service.sleepTime(overdue),
                        service.sleepTime(overdue),
                        service.sleepTime(overdue),
                        service.sleepTime(overdue),
                        service.sleepTime(overdue)))
                .containsExactly(
                        Duration.ofMillis(1),
                        Duration.ofMillis(2),
                        Duration.ofMillis(4),
                        Duration.ofMillis(8),
                        poll,
                        poll);

        assertThat(service.sleepTime(clock.instant().plusMillis(5)))
                .as("a due time ahead")
                .isEqualTo(Duration.ofMillis(5));
        assertThat(service.sleepTime(overdue)).as("starts short again").isEqualTo(Duration.ofMillis(1));
        assertThat(service.sleepTime(overdue)).isEqualTo(Duration.ofMillis(2));
        assertThat(service.sleepTime(null)).as("nothing scheduled").isEqualTo(poll);
        assertThat(service.sleepTime(overdue)).as("starts short again").isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void aDueTimeLessThanAMillisecondAheadIsWaitedForAndStartsTheWaitShortAgain() {
        var service = service(store, Duration.ofMillis(10));
        var overdue = clock.instant().minusSeconds(2);
        for (int i = 0; i < 5; i++) {
            service.sleepTime(overdue);
        }

        assertThat(service.sleepTime(clock.instant().plusNanos(500_000))).isEqualTo(Duration.ofMillis(1));
        assertThat(service.sleepTime(overdue)).as("starts short again").isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void aPromotionThatMovesTasksStartsTheWaitForAnOverdueTaskShortAgain() {
        var running = new AtomicReference<ScheduledTaskService>();
        var waits = new ArrayList<Duration>();
        var promotes = new AtomicInteger();
        // the store answers as one whose clock is behind: a due time that has passed here, and nothing moved
        var behind = StoreInterceptor.around(store, (method, args, real) -> {
            if (!method.equals("promote")) return real.proceed();
            int round = promotes.incrementAndGet();
            if (round == 4) running.get().stopThread();
            return new PromoteResult(round == 3 ? 1 : 0, clock.instant().minusSeconds(2));
        });
        var builder = new ScheduledTaskService.Builder()
                .setGroup(new ThreadGroup("promotion-test"))
                .setThreadName("promotion-test")
                .setStore(behind)
                .setClock(clock)
                .setStatus(status)
                .setPollInterval(Duration.ofMillis(50));
        var service = new ScheduledTaskService(builder) {
            @Override
            Duration sleepTime(Instant nextDue) {
                var wait = super.sleepTime(nextDue);
                waits.add(wait);
                return wait;
            }
        };
        running.set(service);

        service.run();

        assertThat(waits)
                .containsExactly(
                        Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(1), Duration.ofMillis(2));
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
        assertThat(status.issues()).extracting(Issue::kind).containsExactly(Issue.Kind.STORE_ERROR);
    }
}
