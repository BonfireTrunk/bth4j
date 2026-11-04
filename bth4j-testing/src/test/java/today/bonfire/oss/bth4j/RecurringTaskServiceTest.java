package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * One round of the recurring service at a time, run on the test thread against an in-memory store and a manual
 * clock, so that misfire thresholds and the instants that become occurrences are exact.
 */
@Timeout(10)
class RecurringTaskServiceTest {
    private static final TaskType<Void> TYPE = TaskType.of("recurring-test");
    private static final String ID = "job";
    private static final Instant T0 = Instant.parse("2030-01-01T00:00:00Z");

    private final MutableClock clock = new MutableClock(T0);
    private final InMemoryStore store = new InMemoryStore(clock);
    private final RunnerStatus status = new RunnerStatus(clock);
    private final List<TaskEvent> events = new ArrayList<>();
    private final int typeId = store.typeId(TYPE.name());
    private final int queueId = store.queueId("Q:DEF");
    private FullStore serviceStore = store;
    private Duration checkInterval = Duration.ofSeconds(10);
    private Observers observers = new Observers(events::add);

    // ---- misfire threshold ----------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
        "10, 59999, false",
        "10, 60000, false",
        "10, 60001, true",
        "45, 60001, false",
        "45, 90000, false",
        "45, 90001, true",
    })
    void aFireIsAMisfireOnlyBeyondTheLargerOfTwiceTheCheckIntervalAndOneMinute(
            long checkSeconds, long lateMillis, boolean misfired) throws Exception {
        checkInterval = Duration.ofSeconds(checkSeconds);
        clock.set(T0.plus(Duration.ofMinutes(30)));
        var fireAt = clock.instant().minusMillis(lateMillis);
        register(definition("0 * * * *", RecurringDefinition.MISFIRE_SKIP, 0), fireAt);

        round();

        var nextHour = T0.plus(Duration.ofHours(1));
        assertThat(store.recurringSchedules()).containsEntry(ID, nextHour);
        if (misfired) {
            assertThat(occurrences()).isEmpty();
            assertThat(events).isEmpty();
            assertThat(status.issues()).singleElement().satisfies(i -> {
                assertThat(i.kind()).isEqualTo("schedule-misfire");
                assertThat(i.detail()).endsWith("skipped, next at " + nextHour);
            });
        } else {
            assertThat(occurrences())
                    .singleElement()
                    .satisfies(o -> assertThat(o.due()).isEqualTo(fireAt));
            assertThat(events).singleElement().isInstanceOf(TaskEvent.ScheduleFired.class);
            assertThat(status.issues()).isEmpty();
        }
    }

    @Test
    void aFireThatIsNotLateIsCommittedWithTheEventForItsFireTime() throws Exception {
        register(definition("0 * * * *", null, 0), T0);

        round();

        var occurrence = occurrences().getFirst();
        assertThat(occurrence.scheduleId()).isEqualTo(ID);
        assertThat(occurrence.due()).isEqualTo(T0);
        assertThat(events).containsExactly(new TaskEvent.ScheduleFired(ID, occurrence.id(), T0));
        assertThat(store.recurringSchedules()).containsEntry(ID, T0.plus(Duration.ofHours(1)));
    }

    // ---- catch-up -------------------------------------------------------------------------------------

    @Test
    void aCatchUpWithMoreMissedFiresThanAllowedFirstSkipsToTheLastAllowedOnes() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(35)));
        register(definition("*/10 * * * *", RecurringDefinition.MISFIRE_CATCH_UP, 3), T0);

        round();

        var resume = T0.plus(Duration.ofMinutes(10));
        assertThat(occurrences()).isEmpty();
        assertThat(events).isEmpty();
        assertThat(store.recurringSchedules()).containsEntry(ID, resume);
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo("schedule-misfire");
            assertThat(i.detail()).endsWith("catching up the last 3 fires from " + resume);
        });
    }

    @Test
    void theNextRoundCreatesAnOccurrenceForEachOfTheLastAllowedMissedFires() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(35)));
        register(definition("*/10 * * * *", RecurringDefinition.MISFIRE_CATCH_UP, 3), T0);
        round();

        round();

        var missed = List.of(
                T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(20)), T0.plus(Duration.ofMinutes(30)));
        assertThat(occurrences()).extracting(TaskRecord::due).containsExactlyElementsOf(missed);
        assertThat(events)
                .extracting(e -> ((TaskEvent.ScheduleFired) e).fireAt())
                .containsExactlyElementsOf(missed);
        assertThat(store.recurringSchedules()).containsEntry(ID, T0.plus(Duration.ofMinutes(40)));
    }

    @Test
    void aCatchUpWithExactlyTheAllowedMissedFiresCreatesThemAtOnce() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(25)));
        register(definition("*/10 * * * *", RecurringDefinition.MISFIRE_CATCH_UP, 3), T0);

        round();

        assertThat(occurrences())
                .extracting(TaskRecord::due)
                .containsExactly(T0, T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(20)));
        assertThat(store.recurringSchedules()).containsEntry(ID, T0.plus(Duration.ofMinutes(30)));
        assertThat(status.issues()).noneMatch(i -> i.detail().contains("catching up"));
    }

    // ---- fire once now --------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"FIRE_ONCE_NOW", "unknown"})
    void aMisfireThatFiresOnceNowCommitsOneOccurrenceDueNowAndMovesOn(String policy) throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(35)));
        register(definition("*/10 * * * *", policy, 0), T0);
        var committed = capturingCommits();

        round();

        assertThat(committed).singleElement().satisfies(o -> assertThat(o.due()).isEqualTo(clock.instant()));
        assertThat(occurrences()).hasSize(1);
        assertThat(events)
                .singleElement()
                .satisfies(
                        e -> assertThat(((TaskEvent.ScheduleFired) e).fireAt()).isEqualTo(T0));
        assertThat(store.recurringSchedules()).containsEntry(ID, T0.plus(Duration.ofMinutes(40)));
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo("schedule-misfire");
            assertThat(i.detail()).endsWith("firing once now");
        });
    }

    @Test
    void aMisfireWithoutAPolicyFiresOnceNow() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(35)));
        register(definition("*/10 * * * *", null, 0), T0);
        var committed = capturingCommits();

        round();

        assertThat(committed).singleElement().satisfies(o -> assertThat(o.due()).isEqualTo(clock.instant()));
        assertThat(occurrences()).hasSize(1);
    }

    // ---- rejected and failed commits ------------------------------------------------------------------

    @Test
    void aCommitRejectedBecauseTheDefinitionChangedEmitsNoFireEvent() throws Exception {
        register(definition("0 * * * *", null, 0), T0);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("commitRecurring")) {
                store.putRecurring(definition("30 * * * *", null, 0), T0.plus(Duration.ofMinutes(30)), T0);
            }
            return real.proceed();
        });

        round();

        assertThat(occurrences()).isEmpty();
        assertThat(events).isEmpty();
        assertThat(status.issues()).isEmpty();
    }

    @Test
    void aMisfireSkipRejectedBecauseTheDefinitionChangedRaisesNoIssue() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(30)));
        register(definition("0 * * * *", RecurringDefinition.MISFIRE_SKIP, 0), T0);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("setNextFire")) {
                store.putRecurring(definition("30 * * * *", null, 0), T0.plus(Duration.ofMinutes(60)), T0);
            }
            return real.proceed();
        });

        round();

        assertThat(occurrences()).isEmpty();
        assertThat(status.issues()).isEmpty();
    }

    @Test
    void anOccurrenceSkippedBecauseThePreviousOneIsRunningRaisesAnIssueAndNoFireEvent() throws Exception {
        register(definition("0 * * * *", null, 0, true), T0);
        round();
        assertThat(events).hasSize(1);
        events.clear();
        clock.set(T0.plus(Duration.ofHours(1)));

        round();

        assertThat(occurrences()).hasSize(1);
        assertThat(events).isEmpty();
        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo("schedule-overlap"));
        assertThat(store.recurringSchedules()).containsEntry(ID, T0.plus(Duration.ofHours(2)));
    }

    @Test
    void aDefinitionThatCannotBeEvaluatedIsSkippedAndTheOthersStillFire() throws Exception {
        register(
                new RecurringDefinition(
                        "broken",
                        typeId,
                        0,
                        queueId,
                        null,
                        "not a cron",
                        null,
                        null,
                        0,
                        null,
                        false,
                        null,
                        3,
                        null,
                        false,
                        0),
                T0);
        register(definition("0 * * * *", null, 0), T0.plusSeconds(1));

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.scheduleId()).isEqualTo(ID));
        assertThat(events).singleElement().isInstanceOf(TaskEvent.ScheduleFired.class);
        assertThat(store.recurringSchedules()).containsEntry("broken", T0);
    }

    @Test
    void aStoreFailureEndsTheRoundAndTheNextRoundCreatesTheOccurrenceOnce() throws Exception {
        register(definition("0 * * * *", null, 0), T0);
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("dueRecurring") && failing.getAndDecrement() > 0) {
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });

        assertThatThrownBy(this::round).isInstanceOf(TaskStoreException.class);
        assertThat(occurrences()).isEmpty();

        round();
        round();

        assertThat(occurrences()).hasSize(1);
        assertThat(events).hasSize(1);
    }

    @Test
    void aCommitThatFailsForOneRoundIsRetriedByTheNext() throws Exception {
        register(definition("0 * * * *", null, 0), T0);
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("commitRecurring") && failing.getAndDecrement() > 0) {
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });

        round();

        assertThat(occurrences()).isEmpty();
        assertThat(events).isEmpty();
        assertThat(store.recurringSchedules()).containsEntry(ID, T0);

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.due()).isEqualTo(T0));
        assertThat(events).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"runtime", "fatal"})
    void anObserverFailureDoesNotUndoTheOccurrenceOrStopLaterFires(String kind) throws Exception {
        var failing = new AtomicInteger(1);
        observers = new Observers(event -> {
            events.add(event);
            if (failing.getAndDecrement() > 0) {
                if (kind.equals("runtime")) throw new IllegalStateException("observer broke");
                throw new OutOfMemoryError("observer broke");
            }
        });
        register(definition("0 * * * *", null, 0), T0);

        round();
        round();

        assertThat(occurrences()).hasSize(1);
        assertThat(events).hasSize(1);

        clock.set(T0.plus(Duration.ofHours(1)));
        round();

        assertThat(occurrences()).extracting(TaskRecord::due).containsExactly(T0, T0.plus(Duration.ofHours(1)));
        assertThat(events).hasSize(2);
    }

    // ---- fixtures -------------------------------------------------------------------------------------

    private RecurringDefinition definition(String cron, String misfire, int catchUpMaxFires) {
        return definition(cron, misfire, catchUpMaxFires, false);
    }

    private RecurringDefinition definition(String cron, String misfire, int catchUpMaxFires, boolean overlapSkip) {
        return new RecurringDefinition(
                ID,
                typeId,
                0,
                queueId,
                null,
                cron,
                null,
                misfire,
                catchUpMaxFires,
                null,
                overlapSkip,
                null,
                3,
                null,
                false,
                0);
    }

    private List<TaskRecord> capturingCommits() {
        var committed = new ArrayList<TaskRecord>();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("commitRecurring")) committed.add((TaskRecord) args[2]);
            return real.proceed();
        });
        return committed;
    }

    private void register(RecurringDefinition definition, Instant fireAt) {
        store.putRecurring(definition, fireAt, fireAt);
    }

    private void round() throws Exception {
        PrivateMethods.invoke(service(), "queueDueOccurrences");
    }

    private List<TaskRecord> occurrences() {
        var all = new ArrayList<TaskRecord>();
        for (var state : TaskState.values())
            all.addAll(store.tasks(state, null, null, 100).items());
        all.sort(Comparator.comparing(TaskRecord::due).thenComparingLong(TaskRecord::id));
        return all;
    }

    private RecurringTaskService service() {
        return new RecurringTaskService.Builder()
                .group(new ThreadGroup("recurring-test"))
                .threadName("recurring-test")
                .store(serviceStore)
                .inspection(store)
                .ids(new IdAllocator(store, 10))
                .observer(observers)
                .clock(clock)
                .status(status)
                .checkInterval(checkInterval)
                .create();
    }
}
