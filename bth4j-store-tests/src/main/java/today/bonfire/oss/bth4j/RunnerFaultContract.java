package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.exceptions.EnqueueFailedException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.fault.FaultyStore;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * A {@link BackgroundRunner} and a {@link TaskClient} on a store whose calls fail: at random, for a while, or for
 * good as if the process had died. The tests check what the library promises when a store call is lost before or
 * after the server did its work: every task ends, runs at least once, and exists once. A store module subclasses
 * it and supplies a store whose connection to its server can be cut, as {@link FaultContract} does, with the
 * system clock: the runners take their time from the store, so the leases here are real seconds.
 * <p>
 * The runners use short intervals and a lease of one second so that a test takes seconds. Waiting is done by
 * polling with generous limits.
 */
@Timeout(180)
public abstract class RunnerFaultContract {

    private static final TaskType<Void> WORK = TaskType.of("fault-work");
    private static final Queues QUEUES = Queues.of("Q:FAULT");
    private static final Duration AWAIT_LIMIT = Duration.ofSeconds(90);
    private static final AtomicInteger NAMESPACES = new AtomicInteger();

    private final List<BackgroundRunner> runners = new ArrayList<>();

    /** @return a store of {@code namespace} with no state, isolated from every other namespace, on the system clock */
    protected abstract Faulty newFaulty(String namespace);

    private final List<FaultPlan> plans = new ArrayList<>();

    @AfterEach
    void stopRunners() {
        for (var runner : runners) runner.stop();
        runners.clear();
        plans.forEach(FaultPlan::disarm);
        plans.clear();
    }

    private Faulty faulty() {
        var f = newFaulty("rf" + Long.toString(System.currentTimeMillis(), 36) + NAMESPACES.incrementAndGet());
        f.plan().disarm();
        plans.add(f.plan());
        f.plain().init();
        return f;
    }

    private static TaskClient client(FullStore store) {
        return TaskClient.builder()
                .queues(QUEUES)
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .build();
    }

    private BackgroundRunner runner(FullStore store, TaskProcessor<Void> processor) {
        var runner = BackgroundRunner.builder()
                .queues(QUEUES)
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .concurrency(Concurrency.fixed(8))
                .leaseDuration(Duration.ofSeconds(1))
                .idlePollInterval(Duration.ofMillis(20))
                .maintenanceCheckInterval(Duration.ofSeconds(1))
                .scheduledTaskCheckInterval(Duration.ofMillis(50))
                .scheduleCheckInterval(Duration.ofMillis(200))
                .purgeInterval(Duration.ofSeconds(1))
                .shutdownTimeout(Duration.ofSeconds(5))
                .runnerStaleTimeout(Duration.ofSeconds(3))
                .threadGroupName("rf-" + NAMESPACES.get())
                .retryPolicy(RetryPolicy.fixed(Duration.ofMillis(100)))
                .build();
        runner.registry().register(WORK, processor);
        runners.add(runner);
        return runner;
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + AWAIT_LIMIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            pause(20);
        }
        if (!condition.getAsBoolean()) fail("Timed out after %s waiting for %s", AWAIT_LIMIT, what);
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Repeats a call that the store failed, as an application with an outbox does. */
    private static <T> T untilItWorks(Supplier<T> call) {
        long deadline = System.nanoTime() + AWAIT_LIMIT.toNanos();
        while (true) {
            try {
                return call.get();
            } catch (TaskStoreException | EnqueueFailedException e) {
                if (System.nanoTime() > deadline) throw e;
                pause(10);
            }
        }
    }

    /** Sends a request with an id allocated first, and sends it again with the same id until the store answers. */
    private static EnqueueResult enqueueOnce(TaskClient client, TaskRequest.Builder<Void> request) {
        long id = untilItWorks(client::nextId);
        var built = request.id(id).build();
        return untilItWorks(() -> client.enqueue(built));
    }

    private static List<TaskRecord> allTasks(FullStore store) {
        var all = new ArrayList<TaskRecord>();
        for (TaskState state : TaskState.values()) {
            String cursor = null;
            do {
                var page = store.tasks(state, null, cursor, 100);
                all.addAll(page.items());
                cursor = page.next();
            } while (cursor != null);
        }
        return all;
    }

    private static boolean noOpenTasks(FullStore store) {
        return store.counts().values().stream().allMatch(c -> c.queued() == 0 && c.scheduled() == 0 && c.leased() == 0);
    }

    // random faults

    @Test
    void everyTaskEndsWhenAFewPercentOfTheStoreCallsAreCut() throws Exception {
        runWithRandomFaults(20_261_113L, 0.04, 40);
    }

    @Test
    void everyTaskEndsWhenMoreStoreCallsAreCut() throws Exception {
        runWithRandomFaults(7_351_029L, 0.08, 80);
    }

    /** The seed of a run; {@code -Dbth4j.fault.seed=n} replaces both fixed seeds, to repeat a failing run or to try others. */
    private static long seed(long fixed) {
        return Long.getLong("bth4j.fault.seed", fixed);
    }

    private void runWithRandomFaults(long fixedSeed, double probability, int maxFaults) throws Exception {
        long seed = seed(fixedSeed);
        var f = faulty();
        var plain = f.plain();
        var client = client(f.store());
        var runs = new ConcurrentHashMap<Long, AtomicInteger>();
        var runner = runner(f.store(), (ctx, payload) -> {
            runs.computeIfAbsent(ctx.id(), k -> new AtomicInteger()).incrementAndGet();
            pause(10);
        });

        int plainTasks = 60;
        int delayedTasks = 12;
        int keys = 10;
        var ids = ConcurrentHashMap.<Long>newKeySet();
        var keyHolders = new ConcurrentHashMap<String, Long>();
        var duplicates = new ConcurrentHashMap<String, EnqueueResult>();

        f.plan().failRandomly(seed, probability, maxFaults);
        untilItWorks(() -> {
            runner.start();
            return null;
        });
        var producers = Executors.newFixedThreadPool(4);
        try {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < plainTasks; i++) {
                futures.add(producers.submit(
                        () -> ids.add(enqueueOnce(client, TaskRequest.of(WORK)).id())));
            }
            for (int i = 0; i < delayedTasks; i++) {
                futures.add(producers.submit(() ->
                        ids.add(enqueueOnce(client, TaskRequest.of(WORK).delay(Duration.ofMillis(500)))
                                .id())));
            }
            for (int i = 0; i < keys; i++) {
                var key = "key-" + i;
                futures.add(producers.submit(() -> {
                    var first = enqueueOnce(
                            client, TaskRequest.of(WORK).uniqueKey(key, Uniqueness.keepFor(Duration.ofHours(1))));
                    ids.add(first.id());
                    keyHolders.put(key, first.id());
                    duplicates.put(
                            key,
                            enqueueOnce(
                                    client,
                                    TaskRequest.of(WORK).uniqueKey(key, Uniqueness.keepFor(Duration.ofHours(1)))));
                }));
            }
            futures.add(producers.submit(() -> {
                untilItWorks(() -> client.putSchedule(ScheduleDefinition.every("tick", WORK, Duration.ofHours(1))));
                untilItWorks(() -> plain.triggerSchedule("tick", null, Instant.now()));
            }));
            for (var future : futures) future.get(AWAIT_LIMIT.toSeconds(), TimeUnit.SECONDS);
        } finally {
            producers.shutdownNow();
        }

        int expected = plainTasks + delayedTasks + keys + 1;
        var label = "seed " + seed;
        await("every task to end (" + label + ")", () -> {
            try {
                return plain.counts().values().stream()
                                        .mapToLong(c -> c.succeeded())
                                        .sum()
                                == expected
                        && noOpenTasks(plain);
            } catch (TaskStoreException e) {
                return false;
            }
        });
        int faults = f.plan().faults();
        f.plan().disarm();

        var tasks = allTasks(plain);
        assertThat(tasks).as("tasks stored (%s)", label).hasSize(expected);
        assertThat(tasks.stream().map(TaskRecord::id).distinct().count())
                .as("distinct ids")
                .isEqualTo(expected);
        assertThat(tasks).as("states (%s)", label).allMatch(t -> t.state() == TaskState.SUCCEEDED);
        assertThat(ids).as("ids the producers got").allMatch(id -> runs.containsKey(id));
        for (var t : tasks) {
            assertThat(runs.getOrDefault(t.id(), new AtomicInteger()).get())
                    .as("runs of task %s (%s)", t.id(), label)
                    .isPositive();
        }
        assertThat(tasks.stream().filter(t -> "tick".equals(t.scheduleId())).count())
                .as("occurrences of the schedule (%s)", label)
                .isEqualTo(1);
        for (var key : keyHolders.keySet()) {
            assertThat(tasks.stream().filter(t -> key.equals(t.uniqueKey())).map(TaskRecord::id))
                    .as("holders of %s (%s)", key, label)
                    .containsExactly(keyHolders.get(key));
            assertThat(duplicates.get(key))
                    .as("second enqueue with %s (%s)", key, label)
                    .isEqualTo(new EnqueueResult.Duplicate(keyHolders.get(key)));
        }

        long ranMoreThanOnce = runs.values().stream().filter(n -> n.get() > 1).count();
        System.out.println("runner-fault " + getClass().getSimpleName() + " " + label + ": " + faults + " faults, "
                + expected + " tasks, " + ranMoreThanOnce + " ran more than once");

        var after = enqueueOnce(client, TaskRequest.of(WORK));
        await("the runner to run a task after the faults", () -> runs.containsKey(after.id()));
        assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.RUNNING);
    }

    // a renewal outage

    @Test
    void leasesThatCannotBeRenewedForLongerThanTheLeaseRunAgainAndTheLateAckChangesNothing() throws Exception {
        var f = faulty();
        var plain = f.plain();
        var outage = new AtomicBoolean();
        var store = FaultyStore.around(
                f.store(), method -> outage.get() && method.equals("renew") ? FaultPlan.Cut.BEFORE : null);
        var client = client(plain);
        int tasks = 3;
        var attempts = new ConcurrentHashMap<Long, AtomicInteger>();
        var firstStarted = new CountDownLatch(tasks);
        var secondDone = new ConcurrentHashMap<Long, CountDownLatch>();
        var interrupted = new AtomicInteger();
        runner(store, (ctx, payload) -> {
                    int attempt = attempts.computeIfAbsent(ctx.id(), k -> new AtomicInteger())
                            .incrementAndGet();
                    var done = secondDone.computeIfAbsent(ctx.id(), k -> new CountDownLatch(1));
                    if (attempt == 1) {
                        firstStarted.countDown();
                        try {
                            done.await(60, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            interrupted.incrementAndGet();
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        done.countDown();
                    }
                })
                .start();

        var ids = new ArrayList<Long>();
        for (int i = 0; i < tasks; i++)
            ids.add(enqueueOnce(client, TaskRequest.of(WORK)).id());
        assertThat(firstStarted.await(30, TimeUnit.SECONDS))
                .as("first attempts started")
                .isTrue();

        outage.set(true);
        await(
                "the second attempts to start and end after the leases ran out",
                () -> ids.stream()
                        .allMatch(id -> plain.task(id)
                                .filter(t -> t.state() == TaskState.SUCCEEDED)
                                .isPresent()));
        var afterSecond = new HashMap<Long, TaskRecord>();
        for (var id : ids) afterSecond.put(id, plain.task(id).orElseThrow());
        outage.set(false);

        for (var id : ids) secondDone.get(id).countDown();
        pause(2_500);

        for (var id : ids) {
            assertThat(plain.task(id).orElseThrow())
                    .as("task %s after the first attempt's late ack", id)
                    .isEqualTo(afterSecond.get(id));
            assertThat(afterSecond.get(id).state()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(afterSecond.get(id).attempts()).as("attempts of %s", id).isEqualTo(2);
            assertThat(attempts.get(id).get()).as("runs of %s", id).isEqualTo(2);
        }
        assertThat(noOpenTasks(plain)).isTrue();
    }

    // a process that is gone

    @Test
    void aRunnerThatLosesTheStoreForGoodIsReplacedByASecondOneAfterItsLeasesExpire() throws Exception {
        var f = faulty();
        var plain = f.plain();
        var client = client(plain);
        int tasks = 4;
        var firstRuns = new AtomicInteger();
        var held = new CountDownLatch(1);
        var started = new CountDownLatch(tasks);
        var first = runner(f.store(), (ctx, payload) -> {
            firstRuns.incrementAndGet();
            started.countDown();
            held.await(60, TimeUnit.SECONDS);
        });
        first.start();
        var ids = new ArrayList<Long>();
        for (int i = 0; i < tasks; i++)
            ids.add(enqueueOnce(client, TaskRequest.of(WORK)).id());
        assertThat(started.await(30, TimeUnit.SECONDS))
                .as("tasks running on the first runner")
                .isTrue();
        int firstRunner = first.runnerId();

        f.plan().down();

        var secondRuns = new ConcurrentHashMap<Long, AtomicInteger>();
        runner(
                        plain,
                        (ctx, payload) -> secondRuns
                                .computeIfAbsent(ctx.id(), k -> new AtomicInteger())
                                .incrementAndGet())
                .start();
        await(
                "the second runner to finish every task",
                () -> ids.stream()
                        .allMatch(id -> plain.task(id)
                                .filter(t -> t.state() == TaskState.SUCCEEDED)
                                .isPresent()));
        held.countDown();
        pause(1_500);

        for (var id : ids) {
            var t = plain.task(id).orElseThrow();
            assertThat(t.state()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(t.attempts()).as("attempts of %s", id).isEqualTo(2);
            assertThat(secondRuns.get(id).get())
                    .as("runs of %s on the second runner", id)
                    .isEqualTo(1);
        }
        assertThat(firstRuns.get()).isEqualTo(tasks);
        await(
                "the dead runner's row to be purged",
                () -> plain.runners().stream().noneMatch(r -> r.id() == firstRunner));
    }

    // a lost reply to an enqueue

    private static final class Cuts {
        private final AtomicReference<FaultPlan.Cut> next = new AtomicReference<>();
        private final AtomicInteger cutCalls = new AtomicInteger();

        FaultyStore.Decision decision() {
            return method -> {
                if (!method.equals("enqueue") && !method.equals("enqueueAll")) return null;
                var cut = next.getAndSet(null);
                if (cut != null) cutCalls.incrementAndGet();
                return cut;
            };
        }
    }

    @Test
    void aResendWithTheSameIdAfterALostEnqueueReplyStoresOneTask() {
        var f = faulty();
        var cuts = new Cuts();
        var client = client(FaultyStore.around(f.store(), cuts.decision()));
        long id = client.nextId();
        var request = TaskRequest.of(WORK).id(id).build();

        cuts.next.set(FaultPlan.Cut.AFTER);
        var thrown = org.assertj.core.api.Assertions.catchThrowable(() -> client.enqueue(request));
        assertThat(thrown).isInstanceOf(EnqueueFailedException.class);
        assertThat(((EnqueueFailedException) thrown).ids()).containsExactly(id);
        assertThat(f.plain().task(id)).as("the server stored it").isPresent();

        assertThat(client.enqueue(request)).isEqualTo(new EnqueueResult.Accepted(id, false));
        assertThat(allTasks(f.plain())).extracting(TaskRecord::id).containsExactly(id);
    }

    @Test
    void aResendWithTheSameIdAfterAnEnqueueThatNeverArrivedStoresTheTask() {
        var f = faulty();
        var cuts = new Cuts();
        var client = client(FaultyStore.around(f.store(), cuts.decision()));
        long id = client.nextId();
        var request = TaskRequest.of(WORK).id(id).build();

        cuts.next.set(FaultPlan.Cut.BEFORE);
        assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> client.enqueue(request)))
                .isInstanceOf(EnqueueFailedException.class);
        assertThat(f.plain().task(id)).as("nothing was stored").isEmpty();

        assertThat(client.enqueue(request)).isEqualTo(new EnqueueResult.Accepted(id, true));
        assertThat(allTasks(f.plain())).extracting(TaskRecord::id).containsExactly(id);
    }

    @Test
    void aResendWithTheIdOfAGeneratedRequestAfterALostEnqueueReplyStoresOneTask() {
        var f = faulty();
        var cuts = new Cuts();
        var client = client(FaultyStore.around(f.store(), cuts.decision()));

        cuts.next.set(FaultPlan.Cut.AFTER);
        var thrown = org.assertj.core.api.Assertions.catchThrowable(
                () -> client.enqueue(TaskRequest.of(WORK).build()));
        assertThat(thrown).isInstanceOf(EnqueueFailedException.class);
        long id = ((EnqueueFailedException) thrown).id();
        assertThat(f.plain().task(id)).isPresent();

        var resend = client.enqueue(TaskRequest.of(WORK).id(id).build());
        assertThat(resend).isEqualTo(new EnqueueResult.Accepted(id, false));
        assertThat(allTasks(f.plain())).hasSize(1);
    }

    @Test
    void aBatchResentAfterALostReplyStoresEachTaskOnce() {
        var f = faulty();
        var cuts = new Cuts();
        var client = client(FaultyStore.around(f.store(), cuts.decision()));
        var batch = List.of(
                TaskRequest.of(WORK).build(),
                TaskRequest.of(WORK).build(),
                TaskRequest.of(WORK).uniqueKey("batch-key").build());

        cuts.next.set(FaultPlan.Cut.AFTER);
        var thrown = org.assertj.core.api.Assertions.catchThrowable(() -> client.enqueueAll(batch));
        assertThat(thrown).isInstanceOf(EnqueueFailedException.class);
        var failed = (EnqueueFailedException) thrown;
        assertThat(allTasks(f.plain())).as("the server stored the batch").hasSize(3);

        var again = client.enqueueAll(failed.retryRequests(batch));

        assertThat(again).hasSize(3).allMatch(r -> r instanceof EnqueueResult.Accepted a && !a.created());
        assertThat(allTasks(f.plain())).hasSize(3);
        assertThat(allTasks(f.plain()).stream().map(TaskRecord::id).distinct().count())
                .isEqualTo(3);
    }

    @Test
    void aBatchOfSeveralChunksResentAfterALostChunkReplyStoresEachTaskOnce() {
        for (var cut : FaultPlan.Cut.values()) {
            var f = faulty();
            var calls = new AtomicInteger();
            var client = client(FaultyStore.around(f.store(), method -> {
                if (!method.equals("enqueueAll")) return null;
                return calls.incrementAndGet() == 2 ? cut : null;
            }));
            var batch = new ArrayList<TaskRequest<?>>();
            for (int i = 0; i < 1100; i++) batch.add(TaskRequest.of(WORK).build());

            var thrown = org.assertj.core.api.Assertions.catchThrowable(() -> client.enqueueAll(batch));

            assertThat(thrown).as("batch cut %s", cut).isInstanceOf(EnqueueFailedException.class);
            var failed = (EnqueueFailedException) thrown;
            assertThat(failed.settled()).as("results of the chunk before").hasSize(500);
            assertThat(allTasks(f.plain()))
                    .as("stored after a cut %s", cut)
                    .hasSize(cut == FaultPlan.Cut.AFTER ? 1000 : 500);

            var rest = client.enqueueAll(failed.retryRequests(batch));

            assertThat(rest).hasSize(600);
            var all = new ArrayList<EnqueueResult>(failed.settled());
            all.addAll(rest);
            assertThat(all).allMatch(EnqueueResult::isAccepted);
            assertThat(all.stream().map(EnqueueResult::id).distinct().count())
                    .as("ids answered")
                    .isEqualTo(1100);
            assertThat(allTasks(f.plain())).as("tasks stored, cut %s", cut).hasSize(1100);
            long created = all.stream()
                    .filter(r -> r instanceof EnqueueResult.Accepted a && a.created())
                    .count();
            assertThat(created)
                    .as("answers that say this call stored the task, cut %s", cut)
                    .isEqualTo(cut == FaultPlan.Cut.AFTER ? 600 : 1100);
        }
    }
}
