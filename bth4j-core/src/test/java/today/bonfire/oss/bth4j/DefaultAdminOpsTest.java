package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.RecurringSchedule;
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskPage;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

class DefaultAdminOpsTest {

    private static final Instant NOW = Instant.parse("2026-06-01T12:00:00Z");
    private static final int DEFAULT_QUEUE_ID = 1;
    private static final int OTHER_QUEUE_ID = 2;
    private static final int TYPE_ID = 7;
    private static final java.util.Set<String> REGISTRY_METHODS =
            java.util.Set.of("queueId", "typeId", "queueNames", "typeNames");

    private final Map<String, Function<Object[], Object>> handlers = new HashMap<>();
    private final List<String> calls = new ArrayList<>();
    private final Map<Integer, String> queueNames = new HashMap<>();
    private final Map<Integer, String> typeNames = new HashMap<>();
    private Supplier<RunnerHealth> health;
    private Supplier<List<Issue>> issues;
    private DefaultAdminOps ops;

    @BeforeEach
    void setUp() {
        queueNames.put(DEFAULT_QUEUE_ID, "Q:DEF");
        queueNames.put(OTHER_QUEUE_ID, "Q:OTHER");
        typeNames.put(TYPE_ID, "send-mail");
        handlers.put(
                "queueId",
                a -> queueNames.entrySet().stream()
                        .filter(e -> e.getValue().equals(a[0]))
                        .findFirst()
                        .orElseThrow()
                        .getKey());
        handlers.put("typeId", a -> TYPE_ID);
        handlers.put("queueNames", a -> queueNames);
        handlers.put("typeNames", a -> typeNames);
        ops = newOps();
    }

    private DefaultAdminOps newOps() {
        var store = (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                    var handler = handlers.get(method.getName());
                    if (handler == null) throw new UnsupportedOperationException(method.getName());
                    var arguments = args == null ? new Object[0] : args;
                    if (!REGISTRY_METHODS.contains(method.getName())) {
                        calls.add(method.getName() + Arrays.asList(arguments));
                    }
                    return handler.apply(arguments);
                });
        var queues = new QueuesHolder(java.util.Set.of("Q:DEF", "Q:OTHER"), List.of("Q:DEF"), "Q:DEF");
        return new DefaultAdminOps(
                store,
                new Names(store),
                queues,
                Clock.fixed(NOW, ZoneOffset.UTC),
                () -> health == null ? null : health.get(),
                () -> issues == null ? List.of() : issues.get());
    }

    private static TaskRecord record(long id, int queueId) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queueId)
                .typeId(TYPE_ID)
                .state(TaskState.QUEUED)
                .build();
    }

    private static RecurringDefinition definition(String id, String cron, Long fixedDelayMs, boolean paused) {
        return definition(id, cron, fixedDelayMs, paused, "Europe/Berlin", RecurringDefinition.MISFIRE_SKIP, 0, true);
    }

    private static RecurringDefinition definition(
            String id,
            String cron,
            Long fixedDelayMs,
            boolean paused,
            String zone,
            String misfire,
            int catchUpMaxFires,
            boolean overlapSkip) {
        return new RecurringDefinition(
                id,
                TYPE_ID,
                1,
                OTHER_QUEUE_ID,
                "acme",
                cron,
                zone,
                misfire,
                catchUpMaxFires,
                fixedDelayMs,
                overlapSkip,
                null,
                5,
                null,
                paused,
                3);
    }

    @Test
    void countsAreKeyedByQueueName() {
        var counts = new StateCounts(1, 2, 3, 4, 5, 6, 7);
        handlers.put("counts", a -> Map.of(DEFAULT_QUEUE_ID, counts));

        assertThat(ops.counts()).containsExactly(Map.entry("Q:DEF", counts));
    }

    @Test
    void countsForAnUnregisteredQueueIdAreKeyedByThePlaceholderName() {
        handlers.put("counts", a -> Map.of(99, StateCounts.ZERO));

        assertThat(ops.counts()).containsOnlyKeys("#99");
    }

    @Test
    void queuesAndWorkersAreReturnedAsTheStoreHasThem() {
        var queueInfos = List.of(new QueueInfo(1, "Q:DEF", false), new QueueInfo(2, "Q:OTHER", true));
        var workerInfos = List.of(new WorkerInfo(4, "w", NOW, NOW, 2, true, Set.of(), Set.of()));
        handlers.put("queues", a -> queueInfos);
        handlers.put("workers", a -> workerInfos);

        assertThat(ops.queues()).isEqualTo(queueInfos);
        assertThat(ops.workers()).isEqualTo(workerInfos);
    }

    @Test
    void queueAndEventNamesComeFromTheStoreRegistry() {
        assertThat(ops.queueNames()).isEqualTo(queueNames);
        assertThat(ops.typeNames()).isEqualTo(typeNames);
    }

    @Test
    void pauseQueueResolvesTheNameToItsId() {
        handlers.put("pauseQueue", a -> null);

        ops.pauseQueue("Q:OTHER", true);
        ops.pauseQueue("Q:OTHER", false);

        assertThat(calls).contains("pauseQueue[2, true]", "pauseQueue[2, false]");
    }

    @Test
    void pauseQueueWithABlankNameTargetsTheDefaultQueue() {
        handlers.put("pauseQueue", a -> null);

        ops.pauseQueue(null, true);

        assertThat(calls).contains("pauseQueue[1, true]");
    }

    @Test
    void pauseQueueRejectsAQueueThatIsNotAvailable() {
        handlers.put("pauseQueue", a -> null);

        assertThatThrownBy(() -> ops.pauseQueue("Q:MISSING", true)).isInstanceOf(TaskValidationException.class);
        assertThat(calls).noneMatch(c -> c.startsWith("pauseQueue"));
    }

    @Test
    void adminResolvesAQueueOtherProcessesRegisteredThatIsNotLocallyAvailable() {
        handlers.put("pauseQueue", a -> null);
        queueNames.put(3, "Q:REMOTE");

        ops.pauseQueue("Q:REMOTE", true);

        assertThat(calls).contains("pauseQueue[3, true]");
    }

    @Test
    void adminDoesNotRegisterAnUnknownQueue() {
        var registered = new ArrayList<Object>();
        handlers.put("queueId", a -> {
            registered.add(a[0]);
            return 9;
        });

        assertThatThrownBy(() -> ops.tasks(TaskState.QUEUED, "Q:MISSING", null, 5))
                .isInstanceOf(TaskValidationException.class)
                .hasMessageContaining("Q:MISSING");
        assertThat(registered).isEmpty();
    }

    @Test
    void taskIsReturnedWithQueueAndTypeNames() {
        handlers.put("getTask", a -> Optional.of(record((Long) a[0], OTHER_QUEUE_ID)));

        var view = ops.task(42).orElseThrow();

        assertThat(view.id()).isEqualTo(42);
        assertThat(view.queue()).isEqualTo("Q:OTHER");
        assertThat(view.type()).isEqualTo("send-mail");
    }

    @Test
    void unknownTaskIsEmpty() {
        handlers.put("getTask", a -> Optional.empty());

        assertThat(ops.task(42)).isEmpty();
    }

    @Test
    void tasksWithoutQueueFilterAskTheStoreForAllQueues() {
        handlers.put("tasks", a -> new TaskPage(List.of(record(1, DEFAULT_QUEUE_ID), record(2, OTHER_QUEUE_ID)), null));

        var views = ops.tasks(TaskState.DEAD, null, null, 50).items();

        assertThat(calls).contains("tasks[DEAD, null, null, 50]");
        assertThat(views).extracting("queue").containsExactly("Q:DEF", "Q:OTHER");
    }

    @Test
    void tasksWithQueueFilterPassTheQueueId() {
        handlers.put("tasks", a -> new TaskPage(List.of(), null));

        ops.tasks(TaskState.QUEUED, "Q:OTHER", null, 5);

        assertThat(calls).contains("tasks[QUEUED, 2, null, 5]");
    }

    @Test
    void cursorTasksMapTheQueueAndReturnTheStoresNextCursor() {
        handlers.put("tasks", a -> new TaskPage(List.of(record(1, OTHER_QUEUE_ID)), "42:1"));

        var page = ops.tasks(TaskState.QUEUED, "Q:OTHER", "7:3", 5);

        assertThat(calls).contains("tasks[QUEUED, 2, 7:3, 5]");
        assertThat(page.items()).extracting("queue").containsExactly("Q:OTHER");
        assertThat(page.next()).isEqualTo("42:1");
    }

    @Test
    void cursorTasksRejectAQueueThatIsNotAvailable() {
        assertThatThrownBy(() -> ops.tasks(TaskState.QUEUED, "Q:MISSING", (String) null, 5))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void retryPassesADuplicateKeyRefusalThrough() {
        handlers.put("retry", a -> RetryOutcome.DUPLICATE);

        assertThat(ops.retry(9, 3)).isEqualTo(RetryOutcome.DUPLICATE);
    }

    @Test
    void retryAndDeleteDelegateWithTheRevisionAndReturnTheStoreAnswer() {
        handlers.put("retry", a -> a[1].equals(3) ? RetryOutcome.RETRIED : RetryOutcome.STALE);
        handlers.put("delete", a -> a[1].equals(4));
        handlers.put("getTask", a -> Optional.empty());

        assertThat(ops.retry(9, 3)).isEqualTo(RetryOutcome.RETRIED);
        assertThat(ops.retry(9, 2)).isEqualTo(RetryOutcome.STALE);
        assertThat(ops.delete(9, 4)).isTrue();
        assertThat(ops.delete(9, 5)).isFalse();
        assertThat(calls).contains("retry[9, 3]", "delete[9, 4, null]");
    }

    @Test
    void cancelReturnsEachStoreOutcomeUnchanged() {
        handlers.put("getTask", a -> Optional.empty());
        for (var outcome : CancelOutcome.values()) {
            handlers.put("cancel", a -> outcome);

            assertThat(ops.cancel(1, 1, "why")).isEqualTo(outcome);
        }
    }

    @Test
    void taskByKeyIsReturnedWithQueueAndTypeNames() {
        handlers.put("taskByKey", a -> Optional.of(record(42, OTHER_QUEUE_ID)));

        var view = ops.taskByKey("invoice-9").orElseThrow();

        assertThat(view.id()).isEqualTo(42);
        assertThat(view.queue()).isEqualTo("Q:OTHER");
        assertThat(calls).contains("taskByKey[invoice-9]");
    }

    @Test
    void taskByKeyIsEmptyWhenNoTaskHoldsTheKey() {
        handlers.put("taskByKey", a -> Optional.empty());

        assertThat(ops.taskByKey("invoice-9")).isEmpty();
    }

    @Test
    void cancelByKeyCancelsTheHolderWithTheRevisionItRead() {
        handlers.put(
                "taskByKey",
                a -> Optional.of(
                        record(42, OTHER_QUEUE_ID).toBuilder().revision(5).build()));
        handlers.put("getTask", a -> Optional.empty());
        handlers.put("cancel", a -> CancelOutcome.REQUESTED);

        assertThat(ops.cancelByKey("invoice-9", "duplicate")).isEqualTo(CancelOutcome.REQUESTED);
        assertThat(calls).contains("cancel[42, 5, duplicate, null]");
    }

    @Test
    void cancelByKeyReportsAChangeBetweenTheReadAndTheCancelAsStale() {
        handlers.put("taskByKey", a -> Optional.of(record(42, OTHER_QUEUE_ID)));
        handlers.put("getTask", a -> Optional.empty());
        handlers.put("cancel", a -> CancelOutcome.STALE);

        assertThat(ops.cancelByKey("invoice-9", "duplicate")).isEqualTo(CancelOutcome.STALE);
    }

    @Test
    void cancelByKeyIsNotFoundAndCancelsNothingWhenNoTaskHoldsTheKey() {
        handlers.put("taskByKey", a -> Optional.empty());

        assertThat(ops.cancelByKey("invoice-9", "duplicate")).isEqualTo(CancelOutcome.NOT_FOUND);
        assertThat(calls).noneMatch(c -> c.startsWith("cancel["));
    }

    @Test
    void cancelByKeyIsFinishedAndCancelsNothingWhenTheHolderHasEnded() {
        for (var state : TaskState.values()) {
            if (!state.isTerminal()) continue;
            handlers.put(
                    "taskByKey",
                    a -> Optional.of(
                            record(42, OTHER_QUEUE_ID).toBuilder().state(state).build()));

            assertThat(ops.cancelByKey("invoice-9", "duplicate")).isEqualTo(CancelOutcome.FINISHED);
        }
        assertThat(calls).noneMatch(c -> c.startsWith("cancel["));
    }

    @Test
    void cancelByKeyOfAWaitingFixedDelayOccurrencePassesThePauseToTheStore() {
        var occurrence = TaskRecord.builder()
                .id(7)
                .queueId(OTHER_QUEUE_ID)
                .typeId(TYPE_ID)
                .state(TaskState.SCHEDULED)
                .uniqueKey("sched:poll")
                .scheduleId("poll")
                .headers(Map.of(RecurringTaskService.FIXED_DELAY_HEADER, "PT5S"))
                .build();
        handlers.put("taskByKey", a -> Optional.of(occurrence));
        handlers.put("getTask", a -> Optional.of(occurrence));
        handlers.put("cancel", a -> CancelOutcome.CANCELLED);

        ops.cancelByKey("sched:poll", "stop");

        assertThat(calls).contains("cancel[7, 0, stop, ScheduleEffect[poll, occurrence 7, pause]]");
    }

    @Test
    void cancelPassesTheReasonToTheStore() {
        handlers.put("cancel", a -> CancelOutcome.CANCELLED);
        handlers.put("getTask", a -> Optional.empty());

        ops.cancel(1, 2, "duplicate");

        assertThat(calls).contains("cancel[1, 2, duplicate, null]");
    }

    @Test
    void cancelWithoutAReasonUsesTheOperatorDefault() {
        handlers.put("cancel", a -> CancelOutcome.CANCELLED);
        handlers.put("getTask", a -> Optional.empty());

        ops.cancel(1, 2, null);
        ops.cancel(1, 2, "   ");

        assertThat(calls).contains("cancel[1, 2, cancelled by operator, null]");
        assertThat(calls.stream().filter(c -> c.equals("cancel[1, 2, cancelled by operator, null]")))
                .hasSize(2);
    }

    @Test
    void cancelOfAWaitingFixedDelayOccurrencePassesThePauseToTheStore() {
        handlers.put("cancel", a -> CancelOutcome.CANCELLED);
        handlers.put(
                "getTask",
                a -> Optional.of(TaskRecord.builder()
                        .id((Long) a[0])
                        .queueId(OTHER_QUEUE_ID)
                        .typeId(TYPE_ID)
                        .state(TaskState.CANCELLED)
                        .scheduleId("poll")
                        .headers(Map.of(RecurringTaskService.FIXED_DELAY_HEADER, "PT5S"))
                        .build()));

        ops.cancel(7, 1, "stop");

        assertThat(calls)
                .containsExactly("getTask[7]", "cancel[7, 1, stop, ScheduleEffect[poll, occurrence 7, pause]]");
    }

    @Test
    void deleteOfAFixedDelayOccurrencePassesThePauseToTheStore() {
        handlers.put("delete", a -> true);
        handlers.put(
                "getTask",
                a -> Optional.of(TaskRecord.builder()
                        .id((Long) a[0])
                        .queueId(OTHER_QUEUE_ID)
                        .typeId(TYPE_ID)
                        .state(TaskState.QUEUED)
                        .scheduleId("poll")
                        .headers(Map.of(RecurringTaskService.FIXED_DELAY_HEADER, "PT5S"))
                        .build()));

        assertThat(ops.delete(7, 1)).isTrue();

        assertThat(calls).containsExactly("getTask[7]", "delete[7, 1, ScheduleEffect[poll, occurrence 7, pause]]");
    }

    @Test
    void cancelOfAnOrdinaryTaskDoesNotTouchSchedules() {
        handlers.put("cancel", a -> CancelOutcome.CANCELLED);
        handlers.put("getTask", a -> Optional.of(record((Long) a[0], OTHER_QUEUE_ID)));

        ops.cancel(7, 1, "stop");

        assertThat(calls).containsExactly("getTask[7]", "cancel[7, 1, stop, null]");
    }

    @Test
    void cancelRequestedOnARunningTaskPassesNoEffect() {
        handlers.put("cancel", a -> CancelOutcome.REQUESTED);
        handlers.put("getTask", a -> Optional.of(record((Long) a[0], OTHER_QUEUE_ID)));

        assertThat(ops.cancel(7, 1, "stop")).isEqualTo(CancelOutcome.REQUESTED);
        assertThat(calls).containsExactly("getTask[7]", "cancel[7, 1, stop, null]");
    }

    @Test
    void cancelRejectsAReasonLongerThan512CharactersWithoutCallingTheStore() {
        handlers.put("cancel", a -> CancelOutcome.CANCELLED);
        handlers.put("getTask", a -> Optional.empty());

        assertThatThrownBy(() -> ops.cancel(1, 2, "x".repeat(513))).isInstanceOf(TaskValidationException.class);
        assertThat(calls).isEmpty();

        assertThat(ops.cancel(1, 2, "x".repeat(512))).isEqualTo(CancelOutcome.CANCELLED);
    }

    @Test
    void purgeStaleWorkersDelegatesAndReturnsTheCount() {
        handlers.put("purgeWorkers", a -> 3);

        assertThat(ops.purgeStaleWorkers(Duration.ofMinutes(5))).isEqualTo(3);
        assertThat(calls).contains("purgeWorkers[PT5M]");
    }

    @Test
    void schedulesAreViewedWithResolvedNames() {
        handlers.put(
                "recurringDefinitions",
                a -> List.of(
                        new RecurringSchedule(definition("nightly", "0 2 * * *", null, false), NOW.plusSeconds(60)),
                        new RecurringSchedule(definition("poll", null, 90_000L, true), NOW)));

        var views = ops.schedules();

        assertThat(views).hasSize(2);
        var nightly = views.get(0);
        assertThat(nightly.id()).isEqualTo("nightly");
        assertThat(nightly.type()).isEqualTo("send-mail");
        assertThat(nightly.queue()).isEqualTo("Q:OTHER");
        assertThat(nightly.account()).isEqualTo("acme");
        assertThat(nightly.cron()).isEqualTo("0 2 * * *");
        assertThat(nightly.zone()).isEqualTo(ZoneId.of("Europe/Berlin"));
        assertThat(nightly.misfire()).isEqualTo(Misfire.SKIP);
        assertThat(nightly.fixedDelay()).isNull();
        assertThat(nightly.overlap()).isEqualTo(Overlap.SKIP);
        assertThat(nightly.paused()).isFalse();
        assertThat(nightly.nextFire()).isEqualTo(NOW.plusSeconds(60));
        assertThat(nightly.maxAttempts()).isEqualTo(5);
        assertThat(views.get(1).fixedDelay()).isEqualTo(Duration.ofSeconds(90));
        assertThat(views.get(1).paused()).isTrue();
    }

    @Test
    void scheduleViewMapsZoneMisfireAndOverlapToTheirTypes() {
        handlers.put(
                "recurringDefinitions",
                a -> List.of(
                        new RecurringSchedule(
                                definition(
                                        "zoned",
                                        "0 2 * * *",
                                        null,
                                        false,
                                        "Europe/Berlin",
                                        RecurringDefinition.MISFIRE_CATCH_UP,
                                        4,
                                        false),
                                NOW),
                        new RecurringSchedule(
                                definition(
                                        "plain", null, 1_000L, false, null, RecurringDefinition.MISFIRE_SKIP, 0, true),
                                NOW)));

        var views = ops.schedules();

        assertThat(views.get(0).zone()).isEqualTo(ZoneId.of("Europe/Berlin"));
        assertThat(views.get(0).misfire()).isEqualTo(Misfire.catchUp(4));
        assertThat(views.get(0).overlap()).isEqualTo(Overlap.ALLOW);
        assertThat(views.get(1).zone()).isNull();
        assertThat(views.get(1).misfire()).isEqualTo(Misfire.SKIP);
        assertThat(views.get(1).overlap()).isEqualTo(Overlap.SKIP);
    }

    @Test
    void pausingAScheduleOnlyTellsTheStore() {
        handlers.put("pauseRecurring", a -> true);

        assertThat(ops.pauseSchedule("poll", true)).isTrue();

        assertThat(calls).containsExactly("pauseRecurring[poll, true]");
    }

    @Test
    void pausingAnUnknownScheduleReturnsFalseAndDoesNothingElse() {
        handlers.put("pauseRecurring", a -> false);
        handlers.put("recurringDefinitions", a -> List.of());

        assertThat(ops.pauseSchedule("missing", false)).isFalse();

        assertThat(calls).containsExactly("recurringDefinitions[]", "pauseRecurring[missing, false]");
    }

    @Test
    void resumingAFixedDelayScheduleRearmsItsNextFireFromNow() {
        handlers.put("pauseRecurring", a -> true);
        handlers.put(
                "recurringDefinitions",
                a -> List.of(
                        new RecurringSchedule(definition("other", null, 1_000L, false), NOW),
                        new RecurringSchedule(definition("poll", null, 90_000L, false), NOW.plusSeconds(999))));
        handlers.put("setNextFire", a -> true);

        assertThat(ops.pauseSchedule("poll", false)).isTrue();

        assertThat(calls)
                .containsExactly(
                        "recurringDefinitions[]",
                        "setNextFire[poll, " + NOW.plusSeconds(90) + "]",
                        "pauseRecurring[poll, false]");
    }

    @Test
    void resumingACronScheduleKeepsItsNextFire() {
        handlers.put("pauseRecurring", a -> true);
        handlers.put(
                "recurringDefinitions",
                a -> List.of(new RecurringSchedule(definition("nightly", "0 2 * * *", null, false), NOW)));
        handlers.put("setNextFire", a -> true);

        assertThat(ops.pauseSchedule("nightly", false)).isTrue();

        assertThat(calls).noneMatch(c -> c.startsWith("setNextFire"));
    }

    @Test
    void triggeringAScheduleSetsItsNextFireToNow() {
        handlers.put("setNextFire", a -> true);

        assertThat(ops.triggerSchedule("nightly")).isTrue();

        assertThat(calls).containsExactly("setNextFire[nightly, " + NOW + "]");
    }

    @Test
    void triggeringAnUnknownScheduleReturnsTheStoreAnswer() {
        handlers.put("setNextFire", a -> false);

        assertThat(ops.triggerSchedule("missing")).isFalse();
    }

    @Test
    void deletingAScheduleDelegatesToTheStore() {
        handlers.put("deleteRecurring", a -> a[0].equals("nightly"));

        assertThat(ops.deleteSchedule("nightly")).isTrue();
        assertThat(ops.deleteSchedule("missing")).isFalse();
    }

    @Test
    void healthIsEmptyWithoutASupplierValue() {
        assertThat(ops.health()).isEmpty();
    }

    @Test
    void healthIsTheSuppliedSnapshot() {
        var snapshot = new RunnerHealth(RunnerHealth.Status.RUNNING, 3, 1, 0, NOW, NOW, null, null, 0);
        health = () -> snapshot;

        assertThat(ops.health()).contains(snapshot);
    }

    @Test
    void healthIsEmptyWhenTheOpsWereBuiltWithoutASupplier() {
        var store = (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (p, m, a) -> null);
        var bare = new DefaultAdminOps(
                store,
                new Names(store),
                new QueuesHolder(java.util.Set.of("Q:DEF"), List.of("Q:DEF"), "Q:DEF"),
                Clock.systemUTC(),
                null,
                null);

        assertThat(bare.health()).isEmpty();
        assertThat(bare.issues()).isEmpty();
    }

    @Test
    void issuesAreTheSuppliedList() {
        var issue = new Issue(NOW, "store-error", "boom", null);
        issues = () -> List.of(issue);

        assertThat(ops.issues()).containsExactly(issue);
    }
}
