package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.InspectionStore;
import today.bonfire.oss.bth4j.spi.StoredSchedule;
import today.bonfire.oss.bth4j.spi.TaskRecord;
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
    private final InMemoryStore store = InMemoryStore.builder().clock(clock).build();
    private final RunnerStatus status = new RunnerStatus(clock);
    private final List<TaskEvent> events = new ArrayList<>();
    private final int typeId = store.typeId(TYPE.name());
    private final int queueId = store.queueId("Q:DEF");
    private FullStore serviceStore = store;
    private InspectionStore inspection = store;
    private Duration checkInterval = Duration.ofSeconds(10);
    private int batchSize = 100;
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
        register(definition("0 * * * *", StoredSchedule.MISFIRE_SKIP, 0), fireAt);

        round();

        var nextHour = T0.plus(Duration.ofHours(1));
        assertThat(store.nextFireTimes()).containsEntry(ID, nextHour);
        if (misfired) {
            assertThat(occurrences()).isEmpty();
            assertThat(events).isEmpty();
            assertThat(status.issues()).singleElement().satisfies(i -> {
                assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_MISFIRE);
                assertThat(i.detail()).endsWith("skipped, next at " + nextHour);
            });
        } else {
            assertThat(occurrences())
                    .singleElement()
                    .satisfies(o -> assertThat(o.dueAt()).isEqualTo(fireAt));
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
        assertThat(occurrence.dueAt()).isEqualTo(T0);
        assertThat(events).containsExactly(new TaskEvent.ScheduleFired(ID, occurrence.id(), T0));
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plus(Duration.ofHours(1)));
    }

    @Test
    void aTriggeredFireIsNotAMisfireHoweverLateItIsTakenUpAndTheFireAfterItIsJudgedAsUsual() throws Exception {
        var nextHour = T0.plus(Duration.ofHours(1));
        register(definition("0 * * * *", StoredSchedule.MISFIRE_SKIP, 0), nextHour);
        var asked = T0.plus(Duration.ofMinutes(5));
        store.triggerSchedule(ID, null, asked);
        clock.set(T0.plus(Duration.ofMinutes(30)));

        round();

        var occurrence = occurrences().getFirst();
        assertThat(occurrence.dueAt()).isEqualTo(asked);
        assertThat(events).containsExactly(new TaskEvent.ScheduleFired(ID, occurrence.id(), asked));
        assertThat(status.issues()).isEmpty();
        assertThat(store.nextFireTimes()).containsEntry(ID, nextHour);

        clock.set(T0.plus(Duration.ofHours(3)).plusSeconds(1));
        round();

        assertThat(occurrences()).hasSize(1);
        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_MISFIRE));
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plus(Duration.ofHours(4)));
    }

    // ---- catch-up -------------------------------------------------------------------------------------

    @Test
    void aCatchUpWithMoreMissedFiresThanAllowedMovesToTheLastAllowedOnesAndCreatesThemInTheSameRound()
            throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(35)));
        register(definition("*/10 * * * *", StoredSchedule.MISFIRE_CATCH_UP, 3), T0);

        round();

        var resume = T0.plus(Duration.ofMinutes(10));
        var missed = List.of(resume, T0.plus(Duration.ofMinutes(20)), T0.plus(Duration.ofMinutes(30)));
        assertThat(occurrences()).extracting(TaskRecord::dueAt).containsExactlyElementsOf(missed);
        assertThat(events)
                .extracting(e -> ((TaskEvent.ScheduleFired) e).fireAt())
                .containsExactlyElementsOf(missed);
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plus(Duration.ofMinutes(40)));
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_MISFIRE);
            assertThat(i.detail()).endsWith("catching up the last 3 fires from " + resume);
        });
    }

    @Test
    void aWeekOfMissedFiresIsCaughtUpInOneStepWithOneIssue() throws Exception {
        var downtime = Duration.ofDays(7);
        var last = T0.plus(downtime);
        clock.set(last.plusSeconds(30));
        register(definition("* * * * *", StoredSchedule.MISFIRE_CATCH_UP, 3), T0);

        round();

        var missed = List.of(last.minusSeconds(120), last.minusSeconds(60), last);
        assertThat(occurrences()).extracting(TaskRecord::dueAt).containsExactlyElementsOf(missed);
        assertThat(store.nextFireTimes()).containsEntry(ID, last.plusSeconds(60));
        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_MISFIRE));
    }

    @Test
    void aCatchUpCreatesTheMissedFiresThatAreLateByLessThanTheThresholdToo() throws Exception {
        clock.set(T0.plusSeconds(60));
        register(definition("* * * * *", StoredSchedule.MISFIRE_CATCH_UP, 5), T0);

        round();

        assertThat(occurrences()).extracting(TaskRecord::dueAt).containsExactly(T0, T0.plusSeconds(60));
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plusSeconds(120));
    }

    @Test
    void aCatchUpWithAHugeMaxFiresDoesNotSizeItsWindowByIt() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(10)));
        register(definition("* * * * *", StoredSchedule.MISFIRE_CATCH_UP, Integer.MAX_VALUE - 1), T0);

        round();

        assertThat(occurrences()).hasSize(11);
    }

    @Test
    void moreMissedFiresThanTheScanLimitFireOnceNowWithAnIssue() throws Exception {
        clock.set(T0.plus(Duration.ofDays(100)));
        register(definition("* * * * *", StoredSchedule.MISFIRE_CATCH_UP, 3), T0);

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.dueAt()).isEqualTo(T0));
        assertThat(store.nextFireTimes()).containsEntry(ID, clock.instant().plusSeconds(60));
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_MISFIRE);
            assertThat(i.detail()).endsWith("fires missed, firing once now");
        });
    }

    @Test
    void aSkippedMisfireCountsAsProgressSoTheRoundGoesOn() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(30)));
        var late = clock.instant().minus(Duration.ofMinutes(10));
        register(definition("0 * * * *", StoredSchedule.MISFIRE_SKIP, 0), late);
        register(definition("other", "*/5 * * * *", null, 0, false), clock.instant());
        checkInterval = Duration.ofMinutes(1);
        batchSize = 1;

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.scheduleId()).isEqualTo("other"));
    }

    @Test
    void aCatchUpWithExactlyTheAllowedMissedFiresCreatesThemAtOnce() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(25)));
        register(definition("*/10 * * * *", StoredSchedule.MISFIRE_CATCH_UP, 3), T0);

        round();

        assertThat(occurrences())
                .extracting(TaskRecord::dueAt)
                .containsExactly(T0, T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(20)));
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plus(Duration.ofMinutes(30)));
        assertThat(status.issues()).noneMatch(i -> i.detail().contains("catching up"));
    }

    @Test
    void aCatchUpCreatesEveryFireItsScanFoundEvenWhenMoreFiresFallDueMeanwhile() throws Exception {
        clock.set(T0.plusSeconds(150));
        register(definition("* * * * *", StoredSchedule.MISFIRE_CATCH_UP, 3), T0);
        var committed = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("commitSchedule") && committed.incrementAndGet() == 1) {
                clock.advance(Duration.ofMinutes(2));
            }
            return result;
        });

        round();

        assertThat(occurrences())
                .extracting(TaskRecord::dueAt)
                .containsExactly(T0, T0.plusSeconds(60), T0.plusSeconds(120), T0.plusSeconds(180), T0.plusSeconds(240));
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plusSeconds(300));
    }

    @Test
    void aDefinitionChangedInTheMiddleOfACatchUpIsScannedAgain() throws Exception {
        clock.set(T0.plusSeconds(150));
        register(definition("* * * * *", StoredSchedule.MISFIRE_CATCH_UP, 3), T0);
        var committed = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("commitSchedule") && committed.incrementAndGet() == 1) {
                register(definition("* * * * *", StoredSchedule.MISFIRE_CATCH_UP, 1), T0.plusSeconds(60));
            }
            return result;
        });

        round();

        assertThat(occurrences()).extracting(TaskRecord::dueAt).containsExactly(T0, T0.plusSeconds(120));
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_MISFIRE);
            assertThat(i.detail()).endsWith("catching up the last 1 fires from " + T0.plusSeconds(120));
        });
    }

    // ---- fire once now --------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"FIRE_ONCE_NOW", "unknown"})
    void aMisfireThatFiresOnceNowStoresOneOccurrenceForTheMissedFireAndMovesOn(String policy) throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(35)));
        register(definition("*/10 * * * *", policy, 0), T0);

        round();

        assertThat(occurrences()).singleElement().satisfies(o -> {
            assertThat(o.dueAt()).isEqualTo(T0);
            assertThat(o.state()).isEqualTo(TaskState.SCHEDULED);
        });
        assertThat(events)
                .singleElement()
                .satisfies(
                        e -> assertThat(((TaskEvent.ScheduleFired) e).fireAt()).isEqualTo(T0));
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plus(Duration.ofMinutes(40)));
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_MISFIRE);
            assertThat(i.detail()).endsWith("firing once now");
        });
    }

    @Test
    void aMisfireWithoutAPolicyFiresOnceNow() throws Exception {
        clock.set(T0.plus(Duration.ofMinutes(35)));
        register(definition("*/10 * * * *", null, 0), T0);

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.dueAt()).isEqualTo(T0));
    }

    // ---- rejected and failed commits ------------------------------------------------------------------

    @Test
    void aCommitRejectedBecauseTheDefinitionChangedEmitsNoFireEvent() throws Exception {
        register(definition("0 * * * *", null, 0), T0);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("commitSchedule")) {
                store.putSchedule(definition("30 * * * *", null, 0), T0.plus(Duration.ofMinutes(30)), T0);
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
        register(definition("0 * * * *", StoredSchedule.MISFIRE_SKIP, 0), T0);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("setNextFire")) {
                store.putSchedule(definition("30 * * * *", null, 0), T0.plus(Duration.ofMinutes(60)), T0);
            }
            return real.proceed();
        });

        round();

        assertThat(occurrences()).isEmpty();
        assertThat(status.issues()).isEmpty();
    }

    @Test
    void anOccurrenceThatIsGoneRightAfterItsFireStillCountsAsFiredAndNotAsSkipped() throws Exception {
        register(definition("0 * * * *", null, 0, true), T0);
        var removed = new ArrayList<Long>();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("commitSchedule")) {
                var occurrence = (TaskRecord) args[2];
                assertThat(store.delete(occurrence.id(), occurrence.revision(), null))
                        .isEqualTo(DeleteOutcome.DELETED);
                removed.add(occurrence.id());
            }
            return result;
        });

        round();

        assertThat(removed).hasSize(1);
        assertThat(events).containsExactly(new TaskEvent.ScheduleFired(ID, removed.getFirst(), T0));
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
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_OVERLAP));
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plus(Duration.ofHours(2)));
    }

    @Test
    void aProtectedFireThatIsStillAheadIsLeftAloneWhileTheEarlierOccurrenceIsOpen() throws Exception {
        register(definition("0 * * * *", null, 0, true), T0);
        round();
        events.clear();
        var nextFire = T0.plus(Duration.ofHours(1));
        clock.set(nextFire.minusMillis(1));

        round();

        assertThat(occurrences()).hasSize(1);
        assertThat(events).isEmpty();
        assertThat(status.issues()).isEmpty();
        assertThat(store.nextFireTimes()).containsEntry(ID, nextFire);
    }

    @Test
    void aProtectedFireIsCreatedAheadOfItsTimeOnceTheEarlierOccurrenceHasEnded() throws Exception {
        register(definition("0 * * * *", null, 0, true), T0);
        round();
        var first = occurrences().getFirst();
        events.clear();
        var nextFire = T0.plus(Duration.ofHours(1));
        clock.set(nextFire.minusSeconds(5));
        round();
        assertThat(store.delete(first.id(), first.revision(), null)).isEqualTo(DeleteOutcome.DELETED);

        round();

        var second = occurrences().getLast();
        assertThat(second.id()).isNotEqualTo(first.id());
        assertThat(second.dueAt()).isEqualTo(nextFire);
        assertThat(second.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(events).containsExactly(new TaskEvent.ScheduleFired(ID, second.id(), nextFire));
        assertThat(status.issues()).isEmpty();
        assertThat(store.nextFireTimes()).containsEntry(ID, T0.plus(Duration.ofHours(2)));
    }

    @Test
    void aFireWithoutProtectionIsCreatedAheadOfItsTimeWhileTheEarlierOccurrenceIsOpen() throws Exception {
        register(definition("0 * * * *", null, 0, false), T0);
        round();
        clock.set(T0.plus(Duration.ofHours(1)).minusSeconds(5));

        round();

        assertThat(occurrences()).hasSize(2);
    }

    @Test
    void withoutAnInspectionStoreAProtectedFireWaitsForItsTime() throws Exception {
        inspection = null;
        var first = T0.plusSeconds(5);
        register(definition("0 * * * *", null, 0, true), first);

        round();
        assertThat(occurrences()).isEmpty();
        clock.set(first);
        round();

        assertThat(occurrences()).hasSize(1);
    }

    @Test
    void aDefinitionThatCannotBeEvaluatedIsPushedAheadWithAnIssueAndTheOthersStillFire() throws Exception {
        register(broken("broken", "not a cron", null), T0);
        register(definition("0 * * * *", null, 0), T0.plusSeconds(1));

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.scheduleId()).isEqualTo(ID));
        assertThat(events).singleElement().isInstanceOf(TaskEvent.ScheduleFired.class);
        assertThat(store.nextFireTimes()).containsEntry("broken", T0.plus(Duration.ofHours(1)));
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_BROKEN);
            assertThat(i.detail()).contains("schedule broken").contains("moved from " + T0);
        });
    }

    @Test
    void aFullBatchOfBrokenDefinitionsDoesNotBlockTheHealthyOneBehindIt() throws Exception {
        batchSize = 2;
        for (int i = 0; i < 5; i++) register(broken("broken-" + i, "not a cron", null), T0.plusMillis(i));
        register(definition("0 * * * *", null, 0), T0.plusSeconds(1));

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.scheduleId()).isEqualTo(ID));
        assertThat(status.issues())
                .hasSize(5)
                .allSatisfy(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_BROKEN));
    }

    @Test
    void aRemovedZoneIsPushedAheadLikeAnInvalidCron() throws Exception {
        register(broken("gone", "0 * * * *", "Mars/Olympus_Mons"), T0);

        round();

        assertThat(store.nextFireTimes()).containsEntry("gone", T0.plus(Duration.ofHours(1)));
        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_BROKEN));
    }

    @Test
    void aBrokenDefinitionWhoseStoreWriteFailsIsNotMovedAndEndsTheRound() throws Exception {
        register(broken("broken", "not a cron", null), T0);
        register(definition("0 * * * *", null, 0), T0.plusSeconds(1));
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("setNextFire")) throw new TaskStoreException("store down");
            return real.proceed();
        });

        round();

        assertThat(store.nextFireTimes()).containsEntry("broken", T0);
        assertThat(occurrences()).isEmpty();
        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR));
    }

    @Test
    void anOccurrenceThatCannotBeReadMovesItsScheduleAheadAndTheRoundGoesOn() throws Exception {
        var fire = T0.plusSeconds(5);
        register(definition("0 * * * *", null, 0, true), fire);
        register(definition("second", "0 * * * *", null, 0, false), fire.plusSeconds(1));
        inspection = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("taskByKey")) {
                throw new UnreadableTaskException(7, "task 7 has a stored value that cannot be read", null);
            }
            return real.proceed();
        });

        round();

        assertThat(store.nextFireTimes().get(ID)).isAfter(fire);
        assertThat(occurrences())
                .singleElement()
                .satisfies(t -> assertThat(t.dueAt()).isEqualTo(fire.plusSeconds(1)));
        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_BROKEN));
    }

    @Test
    void aStoreFailureWhileCommittingIsRecordedAsAStoreErrorAndEndsTheRound() throws Exception {
        register(definition("0 * * * *", null, 0), T0);
        register(definition("second", "0 * * * *", null, 0, false), T0.plusSeconds(1));
        var commits = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("commitSchedule")) {
                commits.incrementAndGet();
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });

        round();

        assertThat(commits).hasValue(1);
        assertThat(status.lastStoreError()).contains("store down");
        assertThat(status.issues())
                .singleElement()
                .satisfies(i -> assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR));
    }

    @Test
    void aStoreFailureEndsTheRoundAndTheNextRoundCreatesTheOccurrenceOnce() throws Exception {
        register(definition("0 * * * *", null, 0), T0);
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("dueSchedules") && failing.getAndDecrement() > 0) {
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
            if (method.equals("commitSchedule") && failing.getAndDecrement() > 0) {
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });

        round();

        assertThat(occurrences()).isEmpty();
        assertThat(events).isEmpty();
        assertThat(store.nextFireTimes()).containsEntry(ID, T0);

        round();

        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.dueAt()).isEqualTo(T0));
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

        assertThat(occurrences()).extracting(TaskRecord::dueAt).containsExactly(T0, T0.plus(Duration.ofHours(1)));
        assertThat(events).hasSize(2);
    }

    // ---- the loop thread ------------------------------------------------------------------------------

    @Test
    void aStoreFailureInARoundIsRecordedAndTheNextRoundStillCreatesTheOccurrence() throws Exception {
        checkInterval = Duration.ofMillis(10);
        register(definition("0 * * * *", null, 0), T0);
        var calls = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("dueSchedules") && calls.incrementAndGet() == 1)
                throw new TaskStoreException("store down");
            return real.proceed();
        });

        runLoopUntil(() -> !occurrences().isEmpty());

        var error = new TaskStoreException("store down").toString();
        assertThat(occurrences())
                .singleElement()
                .satisfies(o -> assertThat(o.scheduleId()).isEqualTo(ID));
        assertThat(calls.get()).isGreaterThanOrEqualTo(2);
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR);
            assertThat(i.detail()).isEqualTo(error);
            assertThat(i.taskId()).isNull();
        });
        assertThat(status.lastStoreError()).isEqualTo(error);
        assertThat(status.lastStoreErrorAt()).isEqualTo(clock.instant());
    }

    @Test
    void everyFailingRoundIsRecordedAndTheLoopKeepsRetryingUntilStopped() throws Exception {
        checkInterval = Duration.ofMillis(10);
        var calls = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("dueSchedules")) {
                calls.incrementAndGet();
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });

        runLoopUntil(() -> calls.get() >= 3);

        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR);
            assertThat(i.detail()).startsWith("today.bonfire.oss.bth4j.exceptions.TaskStoreException: store down");
        });
    }

    @Test
    void anOutageOfTheRecurringLoopLogsOneStackTraceThenOneLinePerFailureAndTheRecovery() throws Exception {
        checkInterval = Duration.ofMillis(10);
        var calls = new AtomicInteger();
        var running = new AtomicReference<RecurringTaskService>();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("dueSchedules")) {
                int call = calls.incrementAndGet();
                if (call <= 3) throw new TaskStoreException("store down");
                if (call == 5) running.get().stopThread();
            }
            return real.proceed();
        });
        var service = service();
        running.set(service);

        try (var logs = new LogCapture(Loggers.RECURRING)) {
            service.run();

            var lines = logs.lines();
            assertThat(lines).hasSize(4);
            assertThat(lines.get(0)).isEqualTo("ERROR Recurring task service Thread error [trace]");
            assertThat(lines.get(1))
                    .isEqualTo("ERROR Recurring task service Thread error (failure 2 in a row: "
                            + "today.bonfire.oss.bth4j.exceptions.TaskStoreException: store down)");
            assertThat(lines.get(2)).contains("failure 3 in a row").doesNotContain("[trace]");
            assertThat(lines.get(3))
                    .startsWith("INFO Recurring task service: the store answers again after 3 failed attempts over ")
                    .endsWith(" seconds");
        }
    }

    @Test
    void aStoreFailureWhileCommittingIsLoggedLikeAnOutageAndARoundThatCommitsEndsIt() throws Exception {
        register(definition("0 * * * *", null, 0), T0);
        var commits = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("commitSchedule") && commits.incrementAndGet() <= 2)
                throw new TaskStoreException("store down");
            return real.proceed();
        });
        var service = service();

        try (var logs = new LogCapture(Loggers.RECURRING)) {
            service.queueDueOccurrences();
            service.queueDueOccurrences();
            service.queueDueOccurrences();

            var lines = logs.lines();
            assertThat(lines.get(0))
                    .isEqualTo("ERROR Recurring task service could not reach the store, ending this round [trace]");
            assertThat(lines.get(1))
                    .isEqualTo(
                            "ERROR Recurring task service could not reach the store, ending this round "
                                    + "(failure 2 in a row: today.bonfire.oss.bth4j.exceptions.TaskStoreException: store down)");
            assertThat(lines.get(2))
                    .startsWith("INFO Recurring task service: the store answers again after 2 failed attempts over ");
            assertThat(lines.get(3)).isEqualTo("INFO Recurring task service queued 1 tasks");
            assertThat(lines).hasSize(4);
        }
    }

    @Test
    void aFatalErrorEndsTheLoopWithoutBeingRecordedAsAStoreError() throws Exception {
        checkInterval = Duration.ofMillis(10);
        var calls = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("dueSchedules")) {
                calls.incrementAndGet();
                throw new OutOfMemoryError("fatal");
            }
            return real.proceed();
        });
        var service = service();
        var escaped = new AtomicReference<Throwable>();
        service.setUncaughtExceptionHandler((t, e) -> escaped.set(e));

        service.start();

        assertThat(service.waitTillDone(Duration.ofSeconds(5))).isTrue();
        service.join(5000);
        assertThat(service.isAlive()).isFalse();
        assertThat(escaped.get()).isInstanceOf(OutOfMemoryError.class).hasMessage("fatal");
        assertThat(calls).hasValue(1);
        assertThat(status.issues()).isEmpty();
        assertThat(status.lastStoreError()).isNull();
    }

    // ---- fixtures -------------------------------------------------------------------------------------

    private StoredSchedule definition(String cron, String misfire, int catchUpMaxFires) {
        return definition(cron, misfire, catchUpMaxFires, false);
    }

    private StoredSchedule definition(String cron, String misfire, int catchUpMaxFires, boolean overlapSkip) {
        return definition(ID, cron, misfire, catchUpMaxFires, overlapSkip);
    }

    private StoredSchedule definition(
            String id, String cron, String misfire, int catchUpMaxFires, boolean overlapSkip) {
        return new StoredSchedule(
                id,
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

    @Test
    void anOccurrenceCarriesTheRunTimeAndTheFixedDelayOfItsSchedule() throws Exception {
        register(
                new StoredSchedule(
                        ID,
                        typeId,
                        0,
                        queueId,
                        null,
                        null,
                        null,
                        null,
                        0,
                        Duration.ofSeconds(90),
                        false,
                        null,
                        3,
                        Duration.ofSeconds(7),
                        false,
                        0),
                T0);

        round();

        var occurrence = occurrences().getFirst();
        assertThat(occurrence.maxRunTime()).isEqualTo(Duration.ofSeconds(7));
        assertThat(occurrence.headers()).containsEntry(RecurringTaskService.FIXED_DELAY_HEADER, "PT1M30S");
    }

    /** A definition whose cron expression or zone cannot be evaluated; the store keeps it as written. */
    private StoredSchedule broken(String id, String cron, String zone) {
        return new StoredSchedule(
                id, typeId, 0, queueId, null, cron, zone, null, 0, null, false, null, 3, null, false, 0);
    }

    private void register(StoredSchedule definition, Instant fireAt) {
        store.putSchedule(definition, fireAt, fireAt);
    }

    private void runLoopUntil(BooleanSupplier condition) throws Exception {
        var service = service();
        service.start();
        try {
            var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
            assertThat(condition.getAsBoolean()).isTrue();
        } finally {
            service.stopThread();
            assertThat(service.waitTillDone(Duration.ofSeconds(5))).isTrue();
        }
    }

    private void round() throws Exception {
        service().queueDueOccurrences();
    }

    private List<TaskRecord> occurrences() {
        var all = new ArrayList<TaskRecord>();
        for (var state : TaskState.values())
            all.addAll(store.tasks(state, null, null, 100).items());
        all.sort(Comparator.comparing(TaskRecord::dueAt).thenComparingLong(TaskRecord::id));
        return all;
    }

    private RecurringTaskService service() {
        return new RecurringTaskService.Builder()
                .group(new ThreadGroup("recurring-test"))
                .threadName("recurring-test")
                .store(serviceStore)
                .inspection(inspection)
                .ids(new IdAllocator(store, 10))
                .observer(observers)
                .clock(clock)
                .status(status)
                .checkInterval(checkInterval)
                .batchSize(batchSize)
                .create();
    }
}
