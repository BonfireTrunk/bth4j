package today.bonfire.oss.bth4j.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.CancelOutcome;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.PutScheduleOutcome;
import today.bonfire.oss.bth4j.PutScheduleResult;
import today.bonfire.oss.bth4j.RetryOutcome;
import today.bonfire.oss.bth4j.ScheduleOutcome;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;

/**
 * What a store leaves behind when a call is cut before the server did its work, or after, so that the caller
 * never got the answer. For each write of the SPI the tests learn how many fault points a clean call has, then
 * run the call again on a fresh, identically prepared namespace with each point cut, before and after, and check
 * that
 * <ul>
 *   <li>the call throws {@link TaskStoreException},
 *   <li>the stored state is the one before the call or the one after a clean call, never something in between,
 *   <li>the caller sending the same call again gets the answer the SPI documents for a resend, and the state ends
 *       as after one clean call.
 * </ul>
 * A store module subclasses it and supplies a store whose connection to its server can be cut, through
 * {@link #newFaulty(String)}. A fault point is a round trip to the server; what a point is depends on the
 * mechanism (see {@link FaultPlan}).
 * <p>
 * Times are compared by presence only, because the namespaces of one sweep are prepared one after the other and
 * their stored times differ. The schedule times the tests choose are fixed instants and are compared as they are.
 */
@Timeout(300)
public abstract class FaultContract {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration SHORT = Duration.ofMillis(200);
    private static final Duration PAST_SHORT = Duration.ofMillis(300);

    /** A fire time that is due on every clock the tests use. */
    private static final Instant FIRE = Instant.parse("2026-01-01T00:00:00Z");

    /** A fire time that is in the future on every clock the tests use. */
    private static final Instant NEXT = Instant.parse("2031-01-01T00:00:00Z");

    /** The next fire time an effect sets: not the one the occurrence's commit set, so that the effect shows. */
    private static final Instant LATER = NEXT.plus(Duration.ofDays(1));

    private static final Instant CREATED = Instant.parse("2025-01-01T00:00:00Z");

    private static final int MANY = 300;

    /** A call with more round trips than this is cut at a sample of them. */
    private static final int MOST_POINTS = 10;

    /** Compares the answer of a resend with the one of a clean call. */
    private static final Object CLEAN = new Object();

    /** @return a store of {@code namespace} with no state, isolated from every other namespace */
    protected abstract Faulty newFaulty(String namespace);

    /** @return the clock the store reads, when the test can control it; otherwise the system clock */
    protected Clock clock() {
        return Clock.systemUTC();
    }

    /** Lets store time pass. Real stores sleep; an in-memory store moves its clock. */
    protected void advance(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private String testName = "";

    @BeforeEach
    void rememberName(TestInfo info) {
        testName = info.getTestMethod().map(m -> m.getName()).orElse(info.getDisplayName());
    }

    // the world of one run

    /** One namespace prepared for one run, with the helpers that prepare and read it. */
    protected final class World {
        final FullStore store;
        final FullStore plain;
        final FaultPlan plan;
        final int queue;
        final int type;
        final int runner;
        private final List<Object[]> watchedKeys = new ArrayList<>();

        World() {
            var faulty = newFaulty("fault" + COUNTER.incrementAndGet() + "x" + System.nanoTime() % 100_000);
            store = faulty.store();
            plain = faulty.plain();
            plan = faulty.plan();
            plain.init();
            queue = plain.queueId("main");
            type = plain.typeId("work");
            runner = plain.allocateRunnerId("fault-test", Set.of(queue), Set.of(type));
        }

        Instant now() {
            return clock().instant();
        }

        TaskRecord.Builder task() {
            return task(plain.allocateIds(1));
        }

        TaskRecord.Builder task(long id) {
            return TaskRecord.builder()
                    .id(id)
                    .queueId(queue)
                    .typeId(type)
                    .maxAttempts(3)
                    .createdAt(CREATED)
                    .encodedPayload("payload");
        }

        long put(TaskRecord.Builder builder) {
            var record = builder.build();
            assertThat(plain.enqueue(record)).isEqualTo(EnqueueOutcome.STORED);
            if (record.uniqueKey() != null) watchedKeys.add(new Object[] {record.id(), record.uniqueKey()});
            return record.id();
        }

        TaskRecord get(long id) {
            return plain.task(id).orElseThrow(() -> new AssertionError("task " + id + " is missing"));
        }

        List<TaskRecord> claim(int count, Duration lease) {
            return plain.claim(
                    new ClaimRequest(List.of(new QueueGrant(queue, count, count)), count, lease, runner, null));
        }

        TaskRecord leased(TaskRecord.Builder builder) {
            put(builder);
            var claimed = claim(1, LEASE);
            assertThat(claimed).hasSize(1);
            return claimed.get(0);
        }

        /** A task whose lease ran out. */
        TaskRecord expired(TaskRecord.Builder builder) {
            put(builder);
            var claimed = claim(1, SHORT);
            assertThat(claimed).hasSize(1);
            advance(PAST_SHORT);
            return claimed.get(0);
        }

        TaskRecord dead(TaskRecord.Builder builder) {
            var t = leased(builder);
            assertThat(plain.fail(t.id(), t.revision(), failure(1), null, true, null))
                    .isEqualTo(FailOutcome.DEAD);
            return get(t.id());
        }

        FailureInfo failure(int attempt) {
            return new FailureInfo(attempt, now(), "java.lang.IllegalStateException", "boom " + attempt, null);
        }

        StoredSchedule cron(String id) {
            return new StoredSchedule(id, type, 1, queue, "acme", "0 3 * * *", "{}", 3, 0);
        }

        StoredSchedule fixedDelay(String id, boolean overlapSkip) {
            return new StoredSchedule(
                    id,
                    type,
                    1,
                    queue,
                    "acme",
                    null,
                    "Europe/Berlin",
                    "CATCH_UP",
                    5,
                    Duration.ofSeconds(90),
                    overlapSkip,
                    "{\"a\":1}",
                    4,
                    Duration.ofMinutes(10),
                    false,
                    0);
        }

        void putDue(StoredSchedule definition) {
            plain.putSchedule(definition, FIRE, FIRE);
        }

        DueSchedule due(String scheduleId) {
            return plain.dueSchedules(NEXT, 10).stream()
                    .filter(d -> d.scheduleId().equals(scheduleId))
                    .findFirst()
                    .orElseThrow();
        }

        TaskRecord occurrence(DueSchedule due) {
            var definition = due.definition();
            var builder = TaskRecord.builder()
                    .id(plain.allocateIds(1))
                    .queueId(definition.queueId())
                    .typeId(definition.typeId())
                    .payloadVersion(definition.payloadVersion())
                    .maxAttempts(definition.maxAttempts())
                    .maxRunTime(definition.maxRunTime())
                    .createdAt(CREATED)
                    .dueAt(due.fireAt())
                    .encodedPayload(definition.encodedPayload())
                    .account(definition.account())
                    .scheduleId(due.scheduleId());
            if (definition.isFixedDelay()) {
                builder.headers(
                        Map.of("bth4j.fixed-delay", definition.fixedDelay().toString()));
            }
            if (definition.overlapSkip()) builder.uniqueKey("sched:" + due.scheduleId());
            return builder.build();
        }

        /** The occurrence of a due fixed-delay definition, stored and queued. */
        TaskRecord queuedOccurrence(String scheduleId) {
            putDue(fixedDelay(scheduleId, false));
            var due = due(scheduleId);
            var occurrence = occurrence(due);
            assertThat(plain.commitSchedule(due, NEXT, occurrence)).isEqualTo(CommitOutcome.STORED);
            return get(occurrence.id());
        }

        TaskRecord leasedOccurrence(String scheduleId) {
            var queued = queuedOccurrence(scheduleId);
            plain.promote(10);
            var claimed = claim(1, LEASE);
            assertThat(claimed).extracting(TaskRecord::id).containsExactly(queued.id());
            return claimed.get(0);
        }

        long revisionOf(String scheduleId) {
            return plain.schedules(null, Integer.MAX_VALUE).stream()
                    .filter(s -> s.definition().scheduleId().equals(scheduleId))
                    .findFirst()
                    .orElseThrow()
                    .definition()
                    .revision();
        }

        // what the tests compare

        Snapshot snapshot() {
            var tasks = new TreeMap<Long, TaskRecord>();
            for (TaskState state : TaskState.values()) {
                String cursor = null;
                do {
                    var page = plain.tasks(state, null, cursor, 100);
                    for (var item : page.items()) tasks.put(item.id(), normal(item));
                    cursor = page.next();
                } while (cursor != null);
            }
            var rest = new LinkedHashMap<String, Object>();
            rest.put("counts", plain.counts());
            rest.put("queues", plain.queues());
            rest.put(
                    "schedules",
                    plain.schedules(null, Integer.MAX_VALUE).stream()
                            .map(e -> new ScheduleEntry(withoutRevision(e.definition()), e.nextFireAt()))
                            .toList());
            rest.put("highWaterMark", plain.idHighWaterMark());
            rest.put("queueNames", plain.queueNames());
            rest.put("typeNames", plain.typeNames());
            rest.put(
                    "runners",
                    plain.runners().stream()
                            .map(r -> new RunnerRecord(
                                    r.id(),
                                    r.label(),
                                    mask(r.startedAt()),
                                    mask(r.lastSeenAt()),
                                    r.running(),
                                    r.accepting(),
                                    r.queueIds(),
                                    r.typeIds()))
                            .toList());
            for (var watched : watchedKeys) {
                var id = (Long) watched[0];
                var key = (String) watched[1];
                rest.put("key " + key, plain.taskByKey(key).map(TaskRecord::id));
                rest.put("key " + key + " held by " + id, plain.holdsKey(id, key));
            }
            return new Snapshot(tasks, rest);
        }
    }

    /** The stored state of a namespace as far as the SPI shows it. */
    record Snapshot(Map<Long, TaskRecord> tasks, Map<String, Object> rest) {}

    /**
     * A revision is a fence that only has to differ from the one before, and a store may skip numbers (the
     * PostgreSQL store draws them from a sequence, which a rolled back transaction leaves used), so states are
     * compared without it. The answers of the resends show that it changed where it had to.
     */
    private static StoredSchedule withoutRevision(StoredSchedule d) {
        return new StoredSchedule(
                d.scheduleId(),
                d.typeId(),
                d.payloadVersion(),
                d.queueId(),
                d.account(),
                d.cron(),
                d.zone(),
                d.misfire(),
                d.catchUpMaxFires(),
                d.fixedDelay(),
                d.overlapSkip(),
                d.encodedPayload(),
                d.maxAttempts(),
                d.maxRunTime(),
                d.paused(),
                0);
    }

    private static Instant mask(Instant instant) {
        return instant == null ? null : Instant.EPOCH;
    }

    private static TaskRecord normal(TaskRecord r) {
        Map<String, StepResult> steps = null;
        if (r.steps() != null) {
            steps = new TreeMap<>();
            for (var e : r.steps().entrySet())
                steps.put(
                        e.getKey(),
                        new StepResult(mask(e.getValue().at()), e.getValue().result()));
        }
        var failures = r.failures() == null
                ? null
                : r.failures().stream()
                        .map(f -> new FailureInfo(f.attempt(), mask(f.at()), f.type(), f.message(), f.stack()))
                        .toList();
        return r.toBuilder()
                .createdAt(mask(r.createdAt()))
                .dueAt(mask(r.dueAt()))
                .stateChangedAt(mask(r.stateChangedAt()))
                .leaseUntil(mask(r.leaseUntil()))
                .claimedAt(mask(r.claimedAt()))
                .keyUntil(mask(r.keyUntil()))
                .startBy(mask(r.startBy()))
                .cancelRequestedAt(mask(r.cancelRequestedAt()))
                .steps(steps)
                .failures(failures)
                .build();
    }

    private static Object answer(Object o) {
        if (o instanceof TaskRecord t) return normal(t);
        if (o instanceof List<?> l) return l.stream().map(FaultContract::answer).toList();
        if (o instanceof PromoteResult p) return new PromoteResult(p.moved(), mask(p.nextDue()));
        if (o instanceof Instant i) return mask(i);
        return o;
    }

    // the sweep

    /** What the caller does: sends the call, through the store whose calls the plan cuts. */
    protected interface Operation {
        Object send(FullStore store);
    }

    /** Prepares a world and returns the call under test. */
    protected interface Scenario {
        Operation prepare(World world);
    }

    /** What the caller does after the cut: sends the call again and checks the answer and the state. */
    protected interface Convergence {
        void check(World world, Operation operation, boolean applied, Object cleanAnswer);
    }

    private static Convergence resends(Object whenApplied) {
        return (w, op, applied, clean) -> {
            var again = answer(op.send(w.store));
            var expected = applied && whenApplied != CLEAN ? whenApplied : clean;
            assertThat(again).as("answer of the resend").isEqualTo(answer(expected));
        };
    }

    /** Cuts a call at every point, and checks it against the clean call as {@link FaultContract} describes. */
    protected void sweep(Scenario scenario, Object resendAnswerWhenApplied) {
        sweep(scenario, resends(resendAnswerWhenApplied), true, false);
    }

    /** {@link #sweep(Scenario, Object)} for a call that may leave some of its tasks done and others not. */
    protected void sweepEachTaskAlone(Scenario scenario, Convergence convergence) {
        sweep(scenario, convergence, true, true);
    }

    protected void sweep(
            Scenario scenario, Convergence convergence, boolean endsAsAfterOneCall, boolean eachTaskAlone) {
        var warmUp = new World();
        scenario.prepare(warmUp).send(warmUp.store);

        var clean = new World();
        var cleanCall = scenario.prepare(clean);
        clean.plan.count();
        Object cleanAnswer;
        try {
            cleanAnswer = cleanCall.send(clean.store);
        } finally {
            clean.plan.disarm();
        }
        int points = clean.plan.reached();
        var after = clean.snapshot();
        if (points == 0) {
            System.out.println("fault-contract " + testName + ": the call makes no round trip, nothing to cut");
            return;
        }

        var chosen = pointsToCut(points);
        System.out.println("fault-contract " + testName + ": " + points + " fault points, cut at " + chosen.size());
        var failures = new ArrayList<String>();
        for (int point : chosen) {
            for (var cut : FaultPlan.Cut.values()) {
                var where = "point " + point + " of " + points + ", cut "
                        + cut.name().toLowerCase();
                try {
                    run(
                            scenario,
                            convergence,
                            endsAsAfterOneCall,
                            eachTaskAlone,
                            point,
                            cut,
                            cleanAnswer,
                            after,
                            where);
                } catch (AssertionError | RuntimeException e) {
                    failures.add(where + ": " + e.getMessage());
                }
            }
        }
        assertThat(failures).as("%s: runs that broke a rule", testName).isEmpty();
    }

    /** Every point, or for a call with very many round trips the first and last few and some in between. */
    private static List<Integer> pointsToCut(int points) {
        var chosen = new java.util.TreeSet<Integer>();
        if (points <= MOST_POINTS) {
            for (int p = 1; p <= points; p++) chosen.add(p);
        } else {
            for (int i = 0; i < 2; i++) {
                chosen.add(1 + i);
                chosen.add(points - i);
            }
            chosen.add(points / 3);
            chosen.add(points * 2 / 3);
        }
        return List.copyOf(chosen);
    }

    private void run(
            Scenario scenario,
            Convergence convergence,
            boolean endsAsAfterOneCall,
            boolean eachTaskAlone,
            int point,
            FaultPlan.Cut cut,
            Object cleanAnswer,
            Snapshot after,
            String where) {
        var w = new World();
        var op = scenario.prepare(w);
        var before = w.snapshot();

        w.plan.cutAt(point, cut);
        Throwable thrown = null;
        Object returned = null;
        try {
            returned = op.send(w.store);
        } catch (Throwable t) {
            thrown = t;
        } finally {
            w.plan.disarm();
        }
        assertThat(w.plan.fired()).as("the cut point was reached").isTrue();
        assertThat(thrown)
                .as("the call cut at %s threw (it returned %s)", where, returned)
                .isInstanceOf(TaskStoreException.class);

        var now = w.snapshot();
        boolean applied = !now.equals(before);
        if (eachTaskAlone) {
            for (var id : now.tasks().keySet()) {
                var t = now.tasks().get(id);
                assertThat(Objects.equals(t, before.tasks().get(id))
                                || Objects.equals(t, after.tasks().get(id)))
                        .as("task %s is as before or as after one clean call: %s", id, t)
                        .isTrue();
            }
            if (!now.equals(before) && !now.equals(after)) {
                System.out.println("fault-contract " + testName + ": " + where + " left some tasks done and some not");
            }
            applied = !now.tasks().equals(before.tasks());
        } else {
            assertThat(now.equals(before) || now.equals(after))
                    .as("state is as before or as after one clean call, but is %s", now)
                    .isTrue();
        }

        convergence.check(w, op, applied, cleanAnswer);
        if (endsAsAfterOneCall) {
            var end = w.snapshot();
            assertThat(end).as("state after the resend").isEqualTo(after);
        }
    }

    // ids and registries

    @Test
    void allocateIdsSurvivesACutAtEveryPoint() {
        sweep(
                w -> s -> s.allocateIds(5),
                (w, op, applied, clean) -> {
                    var first = (Long) op.send(w.store);
                    long cleanFirst = (Long) clean;
                    assertThat(first)
                            .as("first id of the resend")
                            .isGreaterThanOrEqualTo(applied ? cleanFirst + 5 : cleanFirst);
                    assertThat(w.plain.idHighWaterMark()).isGreaterThanOrEqualTo(first + 5);
                },
                false,
                false);
    }

    @Test
    void allocateRunnerIdSurvivesACutAtEveryPoint() {
        sweep(
                w -> s -> s.allocateRunnerId("lost", Set.of(w.queue), Set.of(w.type)),
                (w, op, applied, clean) -> {
                    int again = (Integer) op.send(w.store);
                    var ids = w.plain.runners().stream().map(RunnerRecord::id).toList();
                    assertThat(ids).doesNotHaveDuplicates().contains(again);
                    assertThat(again).isNotEqualTo(w.runner);
                    if (applied) assertThat(again).isNotEqualTo((Integer) clean);
                },
                false,
                false);
    }

    @Test
    void queueIdSurvivesACutAtEveryPoint() {
        sweep(
                w -> s -> s.queueId("fresh-queue"),
                (w, op, applied, clean) -> {
                    int again = (Integer) op.send(w.store);
                    assertThat(w.plain.queueNames()).containsEntry(again, "fresh-queue");
                    assertThat(w.plain.queueNames().values()).containsOnlyOnce("fresh-queue");
                    assertThat(op.send(w.store)).isEqualTo(again);
                },
                false,
                false);
    }

    @Test
    void typeIdSurvivesACutAtEveryPoint() {
        sweep(
                w -> s -> s.typeId("fresh-type"),
                (w, op, applied, clean) -> {
                    int again = (Integer) op.send(w.store);
                    assertThat(w.plain.typeNames()).containsEntry(again, "fresh-type");
                    assertThat(w.plain.typeNames().values()).containsOnlyOnce("fresh-type");
                    assertThat(op.send(w.store)).isEqualTo(again);
                },
                false,
                false);
    }

    // enqueue

    @Test
    void enqueueSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var record = w.task().build();
                    return s -> s.enqueue(record);
                },
                EnqueueOutcome.EXISTS);
    }

    @Test
    void enqueueOfADelayedTaskSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var record =
                            w.task().dueAt(w.now().plus(Duration.ofHours(1))).build();
                    return s -> s.enqueue(record);
                },
                EnqueueOutcome.EXISTS);
    }

    @Test
    void enqueueWithAUniqueKeySurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var record =
                            w.task().uniqueKey("k").keyKeep(Duration.ofHours(1)).build();
                    w.watchedKeys.add(new Object[] {record.id(), "k"});
                    return s -> s.enqueue(record);
                },
                (w, op, applied, clean) -> {
                    var again = op.send(w.store);
                    assertThat(again).isEqualTo(applied ? EnqueueOutcome.EXISTS : clean);
                    var other = w.task(9_999).uniqueKey("k").build();
                    assertThat(w.plain.enqueue(other))
                            .as("another task with the key")
                            .isEqualTo(new EnqueueOutcome.DuplicateKey(
                                    w.plain.taskByKey("k").orElseThrow().id()));
                },
                true,
                false);
    }

    @Test
    void enqueueThatReleasesAnEndedKeyWindowSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var old = w.leased(w.task().uniqueKey("k").keyKeep(SHORT));
                    assertThat(w.plain.ack(old.id(), old.revision(), false, false, null))
                            .isTrue();
                    advance(PAST_SHORT);
                    var record = w.task().uniqueKey("k").build();
                    w.watchedKeys.add(new Object[] {record.id(), "k"});
                    return s -> s.enqueue(record);
                },
                EnqueueOutcome.EXISTS);
    }

    @Test
    void enqueueAllSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var tasks = List.of(
                            w.task().build(),
                            w.task().uniqueKey("a").build(),
                            w.task().dueAt(w.now().plus(Duration.ofHours(1))).build());
                    return s -> s.enqueueAll(tasks);
                },
                List.of(EnqueueOutcome.EXISTS, EnqueueOutcome.EXISTS, EnqueueOutcome.EXISTS));
    }

    @Test
    void enqueueAllOfManyTasksLeavesEachTaskStoredOnceOrNotAtAll() {
        sweepEachTaskAlone(
                w -> {
                    var tasks = new ArrayList<TaskRecord>();
                    for (int i = 0; i < 130; i++) tasks.add(w.task().build());
                    return s -> s.enqueueAll(tasks);
                },
                (w, op, applied, clean) -> {
                    @SuppressWarnings("unchecked")
                    var again = (List<EnqueueOutcome>) op.send(w.store);
                    assertThat(again)
                            .hasSize(130)
                            .allMatch(o -> o == EnqueueOutcome.STORED || o == EnqueueOutcome.EXISTS);
                });
    }

    // claim, lease, ack

    @Test
    void claimSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.put(w.task());
                    w.put(w.task());
                    return s ->
                            s.claim(new ClaimRequest(List.of(new QueueGrant(w.queue, 2, 2)), 2, SHORT, w.runner, null));
                },
                (w, op, applied, clean) -> {
                    var again = answer(op.send(w.store));
                    if (applied) {
                        assertThat(again).as("claim of what is leased already").isEqualTo(List.of());
                        advance(PAST_SHORT);
                        assertThat(w.plain.reclaim(10))
                                .as("leases the caller never saw")
                                .hasSize(2);
                    } else {
                        assertThat(again).isEqualTo(answer(clean));
                    }
                },
                true,
                false);
    }

    @Test
    void claimOfKnownTypesSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.put(w.task());
                    w.put(w.task());
                    return s -> s.claim(new ClaimRequest(
                            List.of(new QueueGrant(w.queue, 2, 2)), 2, SHORT, w.runner, Set.of(w.type)));
                },
                (w, op, applied, clean) -> {
                    var again = answer(op.send(w.store));
                    if (applied) {
                        assertThat(again).as("claim of what is leased already").isEqualTo(List.of());
                        advance(PAST_SHORT);
                        assertThat(w.plain.reclaim(10))
                                .as("leases the caller never saw")
                                .hasSize(2);
                    } else {
                        assertThat(again).isEqualTo(answer(clean));
                    }
                },
                true,
                false);
    }

    @Test
    void renewOfALeaseWithACancelRequestSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    assertThat(w.plain.cancel(t.id(), t.revision(), "stop", null))
                            .isEqualTo(CancelOutcome.REQUESTED);
                    var leases = List.of(new LeaseRef(t.id(), t.revision()));
                    return s -> s.renew(leases, LEASE);
                },
                CLEAN);
    }

    @Test
    void renewSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var a = w.leased(w.task());
                    var b = w.leased(w.task());
                    var leases = List.of(new LeaseRef(a.id(), a.revision()), new LeaseRef(b.id(), b.revision()));
                    return s -> s.renew(leases, LEASE);
                },
                CLEAN);
    }

    @Test
    void renewOfManyLeasesLeavesEachLeaseRenewedOrNot() {
        sweepEachTaskAlone(
                w -> {
                    var leases = manyLeases(w);
                    return s -> s.renew(leases, LEASE);
                },
                resends(CLEAN));
    }

    private List<LeaseRef> manyLeases(World w) {
        var tasks = new ArrayList<TaskRecord>();
        for (int i = 0; i < MANY; i++) tasks.add(w.task().build());
        assertThat(w.plain.enqueueAll(tasks)).hasSize(MANY).containsOnly(EnqueueOutcome.STORED);
        var claimed = w.claim(MANY, LEASE);
        assertThat(claimed).hasSize(MANY);
        return claimed.stream()
                .sorted(java.util.Comparator.comparingLong(TaskRecord::id))
                .map(t -> new LeaseRef(t.id(), t.revision()))
                .toList();
    }

    @Test
    void ackSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    return s -> s.ack(t.id(), t.revision(), true, true, null);
                },
                false);
    }

    @Test
    void ackThatRemovesTheTaskSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
                    return s -> s.ack(t.id(), t.revision(), false, false, null);
                },
                false);
    }

    @Test
    void ackWithAScheduleEffectSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leasedOccurrence("fixed");
                    return s -> s.ack(t.id(), t.revision(), true, true, ScheduleEffect.rearm("fixed", t.id(), LATER));
                },
                false);
    }

    @Test
    void ackAllSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var leases = new ArrayList<LeaseRef>();
                    for (int i = 0; i < 3; i++) {
                        var t = w.leased(w.task());
                        leases.add(new LeaseRef(t.id(), t.revision()));
                    }
                    return s -> s.ackAll(leases, true, true);
                },
                List.of(false, false, false));
    }

    @Test
    void ackAllOfManyLeasesLeavesEachTaskAcknowledgedOrNot() {
        sweepEachTaskAlone(
                w -> {
                    var leases = manyLeases(w);
                    return s -> s.ackAll(leases, true, true);
                },
                (w, op, applied, clean) -> {
                    @SuppressWarnings("unchecked")
                    var again = (List<Boolean>) op.send(w.store);
                    assertThat(again).hasSize(MANY);
                });
    }

    // fail

    @Test
    void failWithARetrySurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    var failure = w.failure(1);
                    var retryAt = w.now().plus(Duration.ofHours(1));
                    return s -> s.fail(t.id(), t.revision(), failure, retryAt, true, null);
                },
                FailOutcome.LOST);
    }

    @Test
    void failToDeadSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    var failure = w.failure(1);
                    return s -> s.fail(t.id(), t.revision(), failure, null, true, null);
                },
                FailOutcome.LOST);
    }

    @Test
    void failThatRemovesTheDeadTaskSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
                    var failure = w.failure(1);
                    return s -> s.fail(t.id(), t.revision(), failure, null, false, null);
                },
                FailOutcome.LOST);
    }

    @Test
    void failToDeadWithAScheduleEffectSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leasedOccurrence("fixed");
                    var failure = w.failure(1);
                    return s ->
                            s.fail(t.id(), t.revision(), failure, null, true, ScheduleEffect.pause("fixed", t.id()));
                },
                FailOutcome.LOST);
    }

    @Test
    void failExpiredSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.expired(w.task());
                    var failure = w.failure(1);
                    var retryAt = w.now().plus(Duration.ofHours(1));
                    return s -> s.failExpired(t.id(), t.revision(), failure, retryAt, true, null);
                },
                FailOutcome.LOST);
    }

    @Test
    void failExpiredToDeadSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.expired(w.task());
                    var failure = w.failure(1);
                    return s -> s.failExpired(t.id(), t.revision(), failure, null, false, null);
                },
                FailOutcome.LOST);
    }

    @Test
    void rescheduleSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    var due = w.now().plus(Duration.ofHours(1));
                    return s -> s.reschedule(t.id(), t.revision(), due);
                },
                false);
    }

    @Test
    void unclaimSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    return s -> s.unclaim(t.id(), t.revision());
                },
                false);
    }

    @Test
    void saveStepsSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    var steps = Map.of("a", new StepResult(w.now(), "one"), "b", new StepResult(w.now(), null));
                    return s -> s.saveSteps(t.id(), t.revision(), steps);
                },
                CLEAN);
    }

    @Test
    void progressSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    return s -> s.progress(t.id(), t.revision(), "half way");
                },
                CLEAN);
    }

    // cancel, delete, retry

    @Test
    void cancelOfAQueuedTaskSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.get(w.put(w.task().uniqueKey("k").keyKeep(Duration.ofHours(1))));
                    return s -> s.cancel(t.id(), t.revision(), "not needed", null);
                },
                CancelOutcome.STALE);
    }

    @Test
    void cancelOfALeasedTaskSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    return s -> s.cancel(t.id(), t.revision(), "not needed", null);
                },
                CLEAN);
    }

    @Test
    void cancelWithAScheduleEffectSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.queuedOccurrence("fixed");
                    return s ->
                            s.cancel(t.id(), t.revision(), "not needed", ScheduleEffect.rearm("fixed", t.id(), LATER));
                },
                CancelOutcome.STALE);
    }

    @Test
    void finishCancelledSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task());
                    assertThat(w.plain.cancel(t.id(), t.revision(), "stop", null))
                            .isEqualTo(CancelOutcome.REQUESTED);
                    return s -> s.finishCancelled(t.id(), t.revision(), "stop", true, null);
                },
                false);
    }

    @Test
    void finishCancelledExpiredSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.expired(w.task());
                    assertThat(w.plain.cancel(t.id(), t.revision(), "stop", null))
                            .isEqualTo(CancelOutcome.REQUESTED);
                    return s -> s.finishCancelledExpired(t.id(), t.revision(), "stop", false, null);
                },
                false);
    }

    @Test
    void deleteSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.get(w.put(w.task().uniqueKey("k").keyKeep(Duration.ofHours(1))));
                    return s -> s.delete(t.id(), t.revision(), null);
                },
                DeleteOutcome.NOT_FOUND);
    }

    @Test
    void deleteWithAScheduleEffectSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.queuedOccurrence("fixed");
                    return s -> s.delete(t.id(), t.revision(), ScheduleEffect.rearm("fixed", t.id(), LATER));
                },
                DeleteOutcome.NOT_FOUND);
    }

    @Test
    void retryOfADeadTaskSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.dead(w.task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
                    return s -> s.retry(t.id(), t.revision());
                },
                RetryOutcome.STALE);
    }

    // queues and runners

    @Test
    void pauseQueueSurvivesACutAtEveryPoint() {
        sweep(
                w -> s -> {
                    s.pauseQueue(w.queue, true);
                    return null;
                },
                CLEAN);
    }

    @Test
    void resumeQueueSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.plain.pauseQueue(w.queue, true);
                    return s -> {
                        s.pauseQueue(w.queue, false);
                        return null;
                    };
                },
                CLEAN);
    }

    @Test
    void heartbeatSurvivesACutAtEveryPoint() {
        sweep(
                w -> s -> {
                    s.heartbeat(w.runner, "fault-test", Set.of(w.queue), Set.of(w.type), 3, false);
                    return null;
                },
                CLEAN);
    }

    @Test
    void heartbeatThatCreatesAPurgedRowAgainSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    advance(PAST_SHORT);
                    assertThat(w.plain.purgeRunners(SHORT)).isPositive();
                    assertThat(w.plain.runners()).isEmpty();
                    return s -> {
                        s.heartbeat(w.runner, "fault-test", Set.of(w.queue), Set.of(w.type), 3, true);
                        return null;
                    };
                },
                CLEAN);
    }

    // maintenance

    @Test
    void promoteSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.put(w.task().dueAt(w.now().plus(SHORT)));
                    w.put(w.task().dueAt(w.now().plus(SHORT)));
                    advance(PAST_SHORT);
                    return s -> s.promote(10);
                },
                new PromoteResult(0, null));
    }

    @Test
    void reclaimLeavesTheStateAsItWasWhenCut() {
        sweep(
                w -> {
                    w.expired(w.task());
                    return s -> s.reclaim(10);
                },
                CLEAN);
    }

    @Test
    void purgeSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    for (int i = 0; i < 2; i++) {
                        var t = w.leased(w.task().uniqueKey("k" + i).keyKeep(Duration.ofHours(1)));
                        assertThat(w.plain.ack(t.id(), t.revision(), true, true, null))
                                .isTrue();
                    }
                    advance(Duration.ofMillis(20));
                    return s -> s.purge(TaskState.SUCCEEDED, Duration.ZERO, 10);
                },
                0L);
    }

    @Test
    void releaseKeysSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    var t = w.leased(w.task().uniqueKey("k").keyKeep(SHORT));
                    assertThat(w.plain.ack(t.id(), t.revision(), false, false, null))
                            .isTrue();
                    advance(PAST_SHORT);
                    return s -> s.releaseKeys(10);
                },
                (w, op, applied, clean) -> {
                    // a released key looks the same as one whose window ended, so the resend may find one or none
                    assertThat((Integer) op.send(w.store)).isIn(0, (Integer) clean);
                },
                true,
                false);
    }

    @Test
    void expireSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.put(w.task().startBy(w.now().plus(SHORT)));
                    w.put(w.task().startBy(w.now().plus(SHORT)).dueAt(w.now().plus(Duration.ofHours(1))));
                    advance(PAST_SHORT);
                    return s -> s.expire(10);
                },
                List.of());
    }

    @Test
    void expireOfTasksWithKeysSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.put(w.task()
                            .uniqueKey("a")
                            .keyKeep(Duration.ofHours(1))
                            .startBy(w.now().plus(SHORT)));
                    w.put(w.task().uniqueKey("b").startBy(w.now().plus(SHORT)));
                    advance(PAST_SHORT);
                    return s -> s.expire(10);
                },
                List.of());
    }

    @Test
    void purgeOfTasksThatHoldKeysSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    for (int i = 0; i < 2; i++) {
                        var t = w.leased(w.task().uniqueKey("k" + i).keyKeep(Duration.ofHours(1)));
                        assertThat(w.plain.fail(t.id(), t.revision(), w.failure(1), null, true, null))
                                .isEqualTo(FailOutcome.DEAD);
                    }
                    advance(Duration.ofMillis(20));
                    return s -> s.purge(TaskState.DEAD, Duration.ZERO, 10);
                },
                0L);
    }

    @Test
    void purgeRunnersSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    advance(PAST_SHORT);
                    return s -> s.purgeRunners(SHORT);
                },
                0);
    }

    // schedules

    @Test
    void putScheduleSurvivesACutAtEveryPoint() {
        sweep(
                w -> s -> s.putSchedule(w.cron("nightly"), NEXT, NEXT),
                new PutScheduleResult(PutScheduleOutcome.UNCHANGED, NEXT));
    }

    @Test
    void putScheduleThatChangesADefinitionSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.plain.putSchedule(w.cron("nightly"), NEXT, NEXT);
                    var changed =
                            new StoredSchedule("nightly", w.type, 1, w.queue, "acme", "0 4 * * *", "{\"v\":2}", 3, 0);
                    return s -> s.putSchedule(changed, NEXT, NEXT);
                },
                new PutScheduleResult(PutScheduleOutcome.UNCHANGED, NEXT));
    }

    @Test
    void putScheduleThatChangesTheTimingDuringARunningOccurrenceSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.queuedOccurrence("fixed");
                    var changed = new StoredSchedule(
                            "fixed",
                            w.type,
                            1,
                            w.queue,
                            "acme",
                            null,
                            "Europe/Berlin",
                            "CATCH_UP",
                            5,
                            Duration.ofSeconds(120),
                            false,
                            "{\"a\":1}",
                            4,
                            Duration.ofMinutes(10),
                            false,
                            0);
                    return s -> s.putSchedule(changed, NEXT, NEXT);
                },
                new PutScheduleResult(PutScheduleOutcome.UNCHANGED, NEXT));
    }

    @Test
    void triggerScheduleDuringARunningOccurrenceSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.queuedOccurrence("fixed");
                    long revision = w.revisionOf("fixed");
                    return s -> s.triggerSchedule("fixed", revision, FIRE);
                },
                ScheduleOutcome.STALE);
    }

    @Test
    void resumeScheduleDuringARunningOccurrenceSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.queuedOccurrence("fixed");
                    assertThat(w.plain.pauseSchedule("fixed", null)).isEqualTo(ScheduleOutcome.APPLIED);
                    long revision = w.revisionOf("fixed");
                    return s -> s.resumeSchedule("fixed", revision, NEXT);
                },
                CLEAN);
    }

    @Test
    void commitScheduleSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.putDue(w.cron("nightly"));
                    var due = w.due("nightly");
                    var occurrence = w.occurrence(due);
                    return s -> s.commitSchedule(due, NEXT, occurrence);
                },
                CommitOutcome.NOT_COMMITTED);
    }

    @Test
    void commitScheduleOfAFixedDelayDefinitionSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.putDue(w.fixedDelay("fixed", false));
                    var due = w.due("fixed");
                    var occurrence = w.occurrence(due);
                    return s -> s.commitSchedule(due, NEXT, occurrence);
                },
                CommitOutcome.NOT_COMMITTED);
    }

    @Test
    void commitScheduleThatSkipsAnOverlapSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.putDue(w.fixedDelay("fixed", true));
                    w.put(w.task().uniqueKey("sched:fixed"));
                    var due = w.due("fixed");
                    var occurrence = w.occurrence(due);
                    return s -> s.commitSchedule(due, NEXT, occurrence);
                },
                CommitOutcome.NOT_COMMITTED);
    }

    @Test
    void setNextFireSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.putDue(w.cron("nightly"));
                    long revision = w.revisionOf("nightly");
                    return s -> s.setNextFire("nightly", revision, FIRE, NEXT);
                },
                false);
    }

    @Test
    void deleteScheduleSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.putDue(w.cron("nightly"));
                    long revision = w.revisionOf("nightly");
                    return s -> s.deleteSchedule("nightly", revision);
                },
                ScheduleOutcome.NOT_FOUND);
    }

    @Test
    void pauseScheduleSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.putDue(w.cron("nightly"));
                    long revision = w.revisionOf("nightly");
                    return s -> s.pauseSchedule("nightly", revision);
                },
                CLEAN);
    }

    @Test
    void resumeScheduleSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.putDue(w.cron("nightly"));
                    assertThat(w.plain.pauseSchedule("nightly", null)).isEqualTo(ScheduleOutcome.APPLIED);
                    long revision = w.revisionOf("nightly");
                    return s -> s.resumeSchedule("nightly", revision, NEXT);
                },
                ScheduleOutcome.STALE);
    }

    @Test
    void triggerScheduleSurvivesACutAtEveryPoint() {
        sweep(
                w -> {
                    w.plain.putSchedule(w.cron("nightly"), NEXT, NEXT);
                    long revision = w.revisionOf("nightly");
                    return s -> s.triggerSchedule("nightly", revision, FIRE);
                },
                ScheduleOutcome.STALE);
    }
}
