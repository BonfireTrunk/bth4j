package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.executor.FixedExecutor;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

@Timeout(10)
class BackgroundRunnerCoverageTest {
    @Test
    void registersOnlyProcessedQueuesAndRegisteredTypesBeforeServicesStart() {
        var clock = new MutableClock();
        var store = new InMemoryStore(clock);
        store.typeId("unregistered");
        var runner = BackgroundRunner.builder()
                .store(store)
                .clock(clock)
                .configureQueues(List.of("main", "mail", "unused"), List.of("mail", "main", "mail"), "main")
                .workerLabel("coverage-test")
                .build();
        runner.registry().register(TaskType.of("send"), (context, payload) -> {});
        runner.registry().register(TaskType.of("export"), (context, payload) -> {});
        try {
            runner.start();

            assertThat(store.workers()).singleElement().satisfies(w -> {
                assertThat(w.id()).isEqualTo(runner.workerId());
                assertThat(w.label()).isEqualTo("coverage-test");
                assertThat(w.queueIds()).containsExactlyInAnyOrder(store.queueId("main"), store.queueId("mail"));
                assertThat(w.typeIds()).containsExactlyInAnyOrder(store.typeId("send"), store.typeId("export"));
            });
        } finally {
            runner.stop();
        }
    }

    @Test
    void aWorkerRowPurgedWhileTheRunnerLivesIsRestoredWithWhatTheRunnerRegistered() throws Exception {
        var clock = new MutableClock();
        var store = new InMemoryStore(clock);
        var heartbeats = new LinkedBlockingQueue<Integer>();
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("heartbeat")) heartbeats.add((int) args[0]);
            return result;
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .clock(clock)
                .configureQueues(List.of("main"), List.of("main"), "main")
                .workerLabel("restored")
                .maintenanceCheckInterval(Duration.ofHours(1))
                .workerStaleAfter(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("send"), (context, payload) -> {});
        try {
            runner.start();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(runner.workerId());
            clock.advance(Duration.ofMinutes(11));
            assertThat(store.purgeWorkers(Duration.ofMinutes(10))).isEqualTo(1);

            runner.resume();

            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(runner.workerId());
            assertThat(store.workers()).singleElement().satisfies(w -> {
                assertThat(w.id()).isEqualTo(runner.workerId());
                assertThat(w.label()).isEqualTo("restored");
                assertThat(w.queueIds()).containsExactly(store.queueId("main"));
                assertThat(w.typeIds()).containsExactly(store.typeId("send"));
                assertThat(w.startedAt()).isEqualTo(clock.instant());
            });
        } finally {
            runner.stop();
        }
    }

    @Test
    void quietAndResumePublishOnTheMaintenanceThreadWithoutWaitingForTheStore() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var heartbeats = new LinkedBlockingQueue<Boolean>();
        var quietHeartbeatEntered = new CountDownLatch(1);
        var releaseQuietHeartbeat = new CountDownLatch(1);
        var caller = Thread.currentThread();
        var heartbeatThreads = new LinkedBlockingQueue<Thread>();
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            if (!method.equals("heartbeat")) return real.proceed();
            boolean accepting = (boolean) args[5];
            if (!accepting) {
                quietHeartbeatEntered.countDown();
                if (!releaseQuietHeartbeat.await(5, TimeUnit.SECONDS))
                    throw new AssertionError("heartbeat not released");
            }
            var result = real.proceed();
            heartbeatThreads.add(Thread.currentThread());
            heartbeats.add(accepting);
            return result;
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .maintenanceCheckInterval(Duration.ofHours(1))
                .workerStaleAfter(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(true);
            runner.quiet();
            assertThat(quietHeartbeatEntered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(store.workers().getFirst().accepting()).isTrue();
            releaseQuietHeartbeat.countDown();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(false);
            assertThat(store.workers().getFirst().accepting()).isFalse();
            runner.resume();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(true);
            assertThat(store.workers().getFirst().accepting()).isTrue();
            assertThat(heartbeatThreads).hasSize(3).doesNotContain(caller);
            assertThat(heartbeatThreads.stream().distinct()).hasSize(1);
        } finally {
            releaseQuietHeartbeat.countDown();
            runner.stop();
        }
    }

    @Test
    void stopPublishesAtOnceThatTheRunnerTakesNoMoreWork() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var published = new LinkedBlockingQueue<Boolean>();
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("heartbeat")) published.add((boolean) args[5]);
            return result;
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .maintenanceCheckInterval(Duration.ofHours(1))
                .workerStaleAfter(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        runner.start();
        assertThat(published.poll(2, TimeUnit.SECONDS)).isEqualTo(true);

        runner.stop();

        assertThat(published.poll(2, TimeUnit.SECONDS)).isEqualTo(false);
        assertThat(store.workers().getFirst().accepting()).isFalse();
    }

    @Test
    void quietBeforeStartIsPublishedByTheInitialHeartbeat() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var published = new LinkedBlockingQueue<Boolean>();
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("heartbeat")) published.add((boolean) args[5]);
            return result;
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .maintenanceCheckInterval(Duration.ofHours(1))
                .workerStaleAfter(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.quiet();
            runner.start();
            assertThat(published.poll(2, TimeUnit.SECONDS)).isEqualTo(false);
            assertThat(store.workers().getFirst().accepting()).isFalse();
        } finally {
            runner.stop();
        }
    }

    @Test
    void aStartWithoutProcessorsFailsWithoutBurningTheStartFlag() {
        var runner = BackgroundRunner.builder()
                .store(new InMemoryStore(new MutableClock()))
                .build();
        try {
            assertThatThrownBy(runner::start).isInstanceOf(TaskConfigurationException.class);

            runner.registry().register(TaskType.of("work"), (context, payload) -> {});
            runner.start();

            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.RUNNING);
        } finally {
            runner.stop();
        }
    }

    @Test
    void aStartWithAJsonTaskTypeAndNoMapperIsRefusedSoThatItsTasksAreNotClaimedAndEndedAsDead() {
        var store = new InMemoryStore(new MutableClock());
        var runner = BackgroundRunner.builder().store(store).build();
        PayloadCodec<String> codec = new PayloadCodec<>() {
            @Override
            public String encode(String payload) {
                return payload;
            }

            @Override
            public String decode(String text) {
                return text;
            }
        };
        runner.registry().register(TaskType.of("coded", codec), (context, payload) -> {});
        runner.registry().register(TaskType.of("plain"), (context, payload) -> {});
        try {
            runner.registry().register(TaskType.of("json", String.class), (context, payload) -> {});
            assertThatThrownBy(runner::start)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("json")
                    .hasMessageContaining("jsonMapper");
            assertThat(store.workers()).isEmpty();
        } finally {
            runner.stop();
        }

        var codecsOnly = BackgroundRunner.builder().store(store).build();
        codecsOnly.registry().register(TaskType.of("coded", codec), (context, payload) -> {});
        codecsOnly.registry().register(TaskType.of("plain"), (context, payload) -> {});
        try {
            assertThatCode(codecsOnly::start).doesNotThrowAnyException();
        } finally {
            codecsOnly.stop();
        }
    }

    @Test
    void aRunnerKeepsTheSettingsItWasBuiltWithWhenItsBuilderIsUsedAgain() {
        var store = new InMemoryStore(new MutableClock());
        var builder = BackgroundRunner.builder()
                .store(store)
                .configureQueues(List.of("main", "mail"), List.of("main", "mail"), "main")
                .queue("main", QueuePolicy.weight(1))
                .workerLabel("first");
        var first = builder.build();
        builder.workerLabel("second")
                .queue("mail", QueuePolicy.weight(5))
                .configureQueues(List.of("other"), null, null);
        first.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            first.start();

            assertThat(store.workers()).singleElement().satisfies(w -> {
                assertThat(w.label()).isEqualTo("first");
                assertThat(w.queueIds()).containsExactlyInAnyOrder(store.queueId("main"), store.queueId("mail"));
            });
        } finally {
            first.stop();
        }
    }

    @Test
    void settingsThatMustAgreeAreCheckedWhenTheRunnerIsBuilt() {
        var store = new InMemoryStore(new MutableClock());

        assertThatThrownBy(() -> BackgroundRunner.builder()
                        .store(store)
                        .concurrency(Concurrency.fixed(2))
                        .taskExecutor(new FixedExecutor(2))
                        .build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("not both");
        assertThatCode(() -> BackgroundRunner.builder()
                        .store(store)
                        .taskExecutor(new FixedExecutor(2))
                        .build())
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> BackgroundRunner.builder()
                        .store(store)
                        .maintenanceCheckInterval(Duration.ofSeconds(10))
                        .workerStaleAfter(Duration.ofSeconds(29))
                        .build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("workerStaleAfter");
        assertThatCode(() -> BackgroundRunner.builder()
                        .store(store)
                        .maintenanceCheckInterval(Duration.ofSeconds(10))
                        .workerStaleAfter(Duration.ofSeconds(30))
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    void aStartThatCannotReachTheStoreLeavesTheRunnerNewAndStartsOnTheNextCall() {
        var down = new AtomicBoolean(true);
        var observed = StoreInterceptor.around(new InMemoryStore(new MutableClock()), (method, args, real) -> {
            if (method.equals("allocateWorkerId") && down.get()) throw new TaskStoreException("store down");
            return real.proceed();
        });
        var runner = BackgroundRunner.builder().store(observed).build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            assertThatThrownBy(runner::start).isInstanceOf(TaskStoreException.class);
            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.NEW);
            assertThat(runner.workerId()).isZero();

            down.set(false);
            runner.start();

            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.RUNNING);
            assertThat(runner.workerId()).isPositive();
        } finally {
            runner.stop();
        }
    }

    @Test
    void aStartThatFailsAfterTheServicesStartedStopsThemAndCanBeRepeated() throws Exception {
        var failing = new AtomicBoolean(true);
        var observed = StoreInterceptor.around(new InMemoryStore(new MutableClock()), (method, args, real) -> {
            if (method.equals("info") && failing.getAndSet(false)) throw new IllegalStateException("info failed");
            return real.proceed();
        });
        var group = "start-failure-test";
        var runner = BackgroundRunner.builder()
                .store(observed)
                .threadGroupName(group)
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            assertThatThrownBy(runner::start).isInstanceOf(IllegalStateException.class);

            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.NEW);
            for (var thread : Thread.getAllStackTraces().keySet()) {
                if (thread.getThreadGroup() != null
                        && thread.getThreadGroup().getName().equals(group)) {
                    thread.join(5_000);
                    assertThat(thread.isAlive()).as(thread.getName()).isFalse();
                }
            }

            runner.start();

            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.RUNNING);
        } finally {
            runner.stop();
        }
    }

    @Test
    void drainWaitsForAClaimThatWasAdmittedBeforeQuietAndForTheTaskItStarts() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var claimEntered = new CountDownLatch(1);
        var claimRelease = new CountDownLatch(1);
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("claim") && !((List<?>) result).isEmpty()) {
                claimEntered.countDown();
                awaitUninterruptibly(claimRelease);
            }
            return result;
        });
        var bodyStarted = new CountDownLatch(1);
        var type = TaskType.of("drain-work");
        var runner = BackgroundRunner.builder().store(observed).build();
        runner.registry().register(type, (context, payload) -> bodyStarted.countDown());
        try {
            runner.start();
            long id = runner.client().enqueue(TaskRequest.of(type).build()).id();
            assertThat(claimEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.LEASED);

            assertThat(runner.drain(Duration.ofMillis(200))).isFalse();
            assertThat(bodyStarted.getCount()).isEqualTo(1);

            claimRelease.countDown();

            assertThat(runner.drain(Duration.ofSeconds(5))).isTrue();
            assertThat(bodyStarted.getCount()).isZero();
            assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        } finally {
            claimRelease.countDown();
            runner.stop();
        }
    }

    @Test
    void stopTakesNoLongerThanOneShutdownTimeoutWhateverNumberOfServicesIsBlocked() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var release = new CountDownLatch(1);
        var blocked = new CountDownLatch(3);
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("claim") || method.equals("promote") || method.equals("dueRecurring")) {
                blocked.countDown();
                awaitUninterruptibly(release);
            }
            return real.proceed();
        });
        var timeout = Duration.ofMillis(500);
        var runner = BackgroundRunner.builder()
                .store(observed)
                .shutdownTimeout(timeout)
                .scheduledTasksCheckInterval(Duration.ofMillis(10))
                .recurringTasksCheckInterval(Duration.ofMillis(10))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();

            var begin = System.nanoTime();
            runner.stop();
            var elapsed = Duration.ofNanos(System.nanoTime() - begin);

            // three blocked services would wait three timeouts one after the other
            assertThat(elapsed).isLessThan(timeout.multipliedBy(2));
        } finally {
            release.countDown();
        }
    }

    @Test
    void aShutdownTimeoutTooLongToCountInNanosecondsStillStopsTheRunner() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var runner = BackgroundRunner.builder()
                .store(store)
                .shutdownTimeout(Duration.ofSeconds(Long.MAX_VALUE))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        runner.start();

        assertThatCode(runner::stop).doesNotThrowAnyException();

        assertThat(store.workers().getFirst().accepting()).isFalse();
        assertThat(runner.drain(Duration.ofSeconds(Long.MAX_VALUE))).isTrue();
    }

    @Test
    void aClaimBatchSizeAtTheIntLimitGivesTheServicesTheLargestBatchNotANegativeOne() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var limits = new LinkedBlockingQueue<Integer>();
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("promote")) limits.add((int) args[0]);
            return real.proceed();
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .claimBatchSize(Integer.MAX_VALUE)
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();

            assertThat(limits.poll(5, TimeUnit.SECONDS)).isEqualTo(Integer.MAX_VALUE);
        } finally {
            runner.stop();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        while (true) {
            try {
                if (latch.await(10, TimeUnit.SECONDS)) return;
                throw new AssertionError("latch not released");
            } catch (InterruptedException ignored) {
                // a store call without a deadline ignores the interrupt of a stop as well
            }
        }
    }
}
