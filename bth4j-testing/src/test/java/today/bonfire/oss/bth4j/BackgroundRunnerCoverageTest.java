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
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.executor.FixedExecutor;
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
                .queues(Queues.of("main", "mail", "unused"))
                .processQueues(List.of("mail", "main", "mail"))
                .runnerLabel("coverage-test")
                .build();
        runner.registry().register(TaskType.of("send"), (context, payload) -> {});
        runner.registry().register(TaskType.of("export"), (context, payload) -> {});
        try {
            runner.start();

            assertThat(store.runners()).singleElement().satisfies(w -> {
                assertThat(w.id()).isEqualTo(runner.runnerId());
                assertThat(w.label()).isEqualTo("coverage-test");
                assertThat(w.queueIds()).containsExactlyInAnyOrder(store.queueId("main"), store.queueId("mail"));
                assertThat(w.typeIds()).containsExactlyInAnyOrder(store.typeId("send"), store.typeId("export"));
            });
        } finally {
            runner.stop();
        }
    }

    @Test
    void aRunnerRowPurgedWhileTheRunnerLivesIsRestoredWithWhatTheRunnerRegistered() throws Exception {
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
                .queues(Queues.of("main"))
                .runnerLabel("restored")
                .maintenanceCheckInterval(Duration.ofHours(1))
                .runnerStaleTimeout(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("send"), (context, payload) -> {});
        try {
            runner.start();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(runner.runnerId());
            clock.advance(Duration.ofMinutes(11));
            assertThat(store.purgeRunners(Duration.ofMinutes(10))).isEqualTo(1);

            runner.resume();

            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(runner.runnerId());
            assertThat(store.runners()).singleElement().satisfies(w -> {
                assertThat(w.id()).isEqualTo(runner.runnerId());
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
                .runnerStaleTimeout(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(true);
            runner.quiet();
            assertThat(quietHeartbeatEntered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(store.runners().getFirst().accepting()).isTrue();
            releaseQuietHeartbeat.countDown();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(false);
            assertThat(store.runners().getFirst().accepting()).isFalse();
            runner.resume();
            assertThat(heartbeats.poll(2, TimeUnit.SECONDS)).isEqualTo(true);
            assertThat(store.runners().getFirst().accepting()).isTrue();
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
                .runnerStaleTimeout(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        runner.start();
        assertThat(published.poll(2, TimeUnit.SECONDS)).isEqualTo(true);

        runner.stop();

        assertThat(published.poll(2, TimeUnit.SECONDS)).isEqualTo(false);
        assertThat(store.runners().getFirst().accepting()).isFalse();
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
                .runnerStaleTimeout(Duration.ofHours(3))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.quiet();
            runner.start();
            assertThat(published.poll(2, TimeUnit.SECONDS)).isEqualTo(false);
            assertThat(store.runners().getFirst().accepting()).isFalse();
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
            assertThat(store.runners()).isEmpty();
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
                .queues(Queues.of("main", "mail"))
                .addQueuePolicy("main", QueuePolicy.ofWeight(1))
                .runnerLabel("first");
        var first = builder.build();
        builder.runnerLabel("second")
                .addQueuePolicy("mail", QueuePolicy.ofWeight(5))
                .queues(Queues.of("other"));
        first.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            first.start();

            assertThat(store.runners()).singleElement().satisfies(w -> {
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
                        .runnerStaleTimeout(Duration.ofSeconds(29))
                        .build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("runnerStaleTimeout");
        assertThatCode(() -> BackgroundRunner.builder()
                        .store(store)
                        .maintenanceCheckInterval(Duration.ofSeconds(10))
                        .runnerStaleTimeout(Duration.ofSeconds(30))
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    void eachFinishedStateIsPurgedWithItsOwnRetention() throws Exception {
        var retentions = new java.util.concurrent.ConcurrentHashMap<TaskState, Duration>();
        var allSeen = new CountDownLatch(1);
        var observed = StoreInterceptor.around(new InMemoryStore(new MutableClock()), (method, args, real) -> {
            if (method.equals("purge")) {
                retentions.put((TaskState) args[0], (Duration) args[1]);
                if (retentions.size() == 4) allSeen.countDown();
            }
            return real.proceed();
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .succeededRetention(Duration.ofMinutes(1))
                .deadRetention(Duration.ofMinutes(2))
                .cancelledRetention(Duration.ofMinutes(3))
                .expiredRetention(Duration.ofMinutes(4))
                .maintenanceCheckInterval(Duration.ofSeconds(1))
                .purgeInterval(Duration.ofMillis(20))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();

            assertThat(allSeen.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(retentions)
                    .containsOnly(
                            java.util.Map.entry(TaskState.SUCCEEDED, Duration.ofMinutes(1)),
                            java.util.Map.entry(TaskState.DEAD, Duration.ofMinutes(2)),
                            java.util.Map.entry(TaskState.CANCELLED, Duration.ofMinutes(3)),
                            java.util.Map.entry(TaskState.EXPIRED, Duration.ofMinutes(4)));
        } finally {
            runner.stop();
        }
    }

    @Test
    void aStartThatCannotReachTheStoreLeavesTheRunnerNewAndStartsOnTheNextCall() {
        var down = new AtomicBoolean(true);
        var observed = StoreInterceptor.around(new InMemoryStore(new MutableClock()), (method, args, real) -> {
            if (method.equals("allocateRunnerId") && down.get()) throw new TaskStoreException("store down");
            return real.proceed();
        });
        var runner = BackgroundRunner.builder().store(observed).build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            assertThatThrownBy(runner::start).isInstanceOf(TaskStoreException.class);
            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.NEW);
            assertThat(runner.runnerId()).isZero();

            down.set(false);
            runner.start();

            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.RUNNING);
            assertThat(runner.runnerId()).isPositive();
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
            assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.LEASED);

            assertThat(runner.drain(Duration.ofMillis(200))).isFalse();
            assertThat(bodyStarted.getCount()).isEqualTo(1);

            claimRelease.countDown();

            assertThat(runner.drain(Duration.ofSeconds(5))).isTrue();
            assertThat(bodyStarted.getCount()).isZero();
            assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
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
            if (method.equals("claim") || method.equals("promote") || method.equals("dueSchedules")) {
                blocked.countDown();
                awaitUninterruptibly(release);
            }
            return real.proceed();
        });
        var timeout = Duration.ofMillis(500);
        var runner = BackgroundRunner.builder()
                .store(observed)
                .shutdownTimeout(timeout)
                .scheduledTaskCheckInterval(Duration.ofMillis(10))
                .scheduleCheckInterval(Duration.ofMillis(10))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();

            var begin = System.nanoTime();
            var ended = runner.stop();
            var elapsed = Duration.ofNanos(System.nanoTime() - begin);

            // three blocked services would wait three timeouts one after the other
            assertThat(elapsed).isLessThan(timeout.multipliedBy(2));
            assertThat(ended).isFalse();
            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.STOPPING);
        } finally {
            release.countDown();
        }
        awaitStatus(runner, RunnerHealth.Status.STOPPED);
        assertThat(runner.stop()).isTrue();
    }

    @Test
    void theRunnerReportsStoppingUntilItsLastTaskHasEndedAlsoAfterTheShutdownTimeoutRanOut() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var runner = BackgroundRunner.builder()
                .store(new InMemoryStore(new MutableClock()))
                .shutdownTimeout(Duration.ofMillis(200))
                .build();
        var type = TaskType.of("work");
        runner.registry().register(type, (context, payload) -> {
            started.countDown();
            awaitUninterruptibly(release);
        });
        try {
            runner.start();
            runner.client().enqueue(TaskRequest.of(type).build());
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(runner.stop()).isFalse();

            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.STOPPING);
            assertThat(runner.stop()).isFalse();
        } finally {
            release.countDown();
        }
        awaitStatus(runner, RunnerHealth.Status.STOPPED);
        assertThat(runner.stop()).isTrue();
    }

    @Test
    void theRunnerReportsStoppingWhileOneOfItsServicesIsStillInAStoreCall() throws Exception {
        var release = new CountDownLatch(1);
        var blocked = new CountDownLatch(1);
        var observed = StoreInterceptor.around(new InMemoryStore(new MutableClock()), (method, args, real) -> {
            if (method.equals("dueSchedules")) {
                blocked.countDown();
                awaitUninterruptibly(release);
            }
            return real.proceed();
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .shutdownTimeout(Duration.ofMillis(300))
                .scheduleCheckInterval(Duration.ofMillis(10))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(runner.stop()).isFalse();

            assertThat(runner.runningTasks()).isZero();
            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.STOPPING);
        } finally {
            release.countDown();
        }
        awaitStatus(runner, RunnerHealth.Status.STOPPED);
    }

    @Test
    void aRunnerWithoutQueuesProcessesTheOneQueueCommon() {
        var store = new InMemoryStore(new MutableClock());
        var runner = BackgroundRunner.builder().store(store).build();
        var type = TaskType.of("work");
        runner.registry().register(type, (context, payload) -> {});
        try {
            runner.start();
            var view = runner.client()
                    .task(runner.client().enqueue(TaskRequest.of(type).build()).id())
                    .orElseThrow();

            assertThat(view.queue()).isEqualTo(Queues.GENERAL);
            assertThat(store.runners().getFirst().queueIds()).containsExactly(store.queueId("general"));
        } finally {
            runner.stop();
        }
    }

    @Test
    void theDefaultQueueIsTheOneOfTheQueuesValueAlsoWhenTheRunnerDoesNotProcessIt() {
        var store = new InMemoryStore(new MutableClock());
        var queues = Queues.of("main", "mail");
        var runner = BackgroundRunner.builder()
                .store(store)
                .queues(queues)
                .processQueues(List.of("mail"))
                .build();
        var producer = TaskClient.builder().store(store).queues(queues).build();
        var type = TaskType.of("work");

        var fromRunner = runner.client()
                .task(runner.client().enqueue(TaskRequest.of(type).build()).id())
                .orElseThrow();
        var fromProducer = producer.task(
                        producer.enqueue(TaskRequest.of(type).build()).id())
                .orElseThrow();

        assertThat(fromRunner.queue()).isEqualTo("main");
        assertThat(fromProducer.queue()).isEqualTo("main");
    }

    @Test
    void aRunnerProcessesOnlyQueuesOfItsQueuesValueAndHasPoliciesOnlyForQueuesItProcesses() {
        var store = new InMemoryStore(new MutableClock());
        var queues = Queues.of("main", "mail");

        assertThatThrownBy(() -> BackgroundRunner.builder()
                        .store(store)
                        .queues(queues)
                        .processQueues(List.of("mail", "reports"))
                        .build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("reports");
        assertThatThrownBy(() -> BackgroundRunner.builder()
                        .store(store)
                        .queues(queues)
                        .processQueues(List.of("mail"))
                        .addQueuePolicy("main", QueuePolicy.ofWeight(2))
                        .build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("does not process");
        assertThatThrownBy(() -> BackgroundRunner.builder().processQueues(List.of()))
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> BackgroundRunner.builder().processQueues(null))
                .isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> BackgroundRunner.builder().queues(null))
                .isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void startOnAStartedRunnerDoesNothing() {
        var runner = BackgroundRunner.builder()
                .store(new InMemoryStore(new MutableClock()))
                .build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        try {
            runner.start();
            var runnerId = runner.runnerId();

            runner.start();

            assertThat(runner.runnerId()).isEqualTo(runnerId);
            assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.RUNNING);
        } finally {
            runner.stop();
        }
    }

    @Test
    void aStoppedRunnerIsStoppedAndCannotBeStartedAgain() {
        var store = new InMemoryStore(new MutableClock());
        var runner = BackgroundRunner.builder().store(store).build();
        runner.registry().register(TaskType.of("work"), (context, payload) -> {});
        runner.start();

        assertThat(runner.stop()).isTrue();

        assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.STOPPED);
        assertThatThrownBy(runner::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("build a new runner");
        assertThat(store.runners()).hasSize(1);
    }

    @Test
    void aRunnerStoppedBeforeItStartedIsStoppedAndCannotBeStarted() {
        var store = new InMemoryStore(new MutableClock());
        var runner = BackgroundRunner.builder().store(store).build();
        assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.NEW);

        assertThat(runner.stop()).isTrue();
        assertThat(runner.stop()).isTrue();

        assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.STOPPED);
        assertThatThrownBy(runner::start).isInstanceOf(IllegalStateException.class);
        assertThat(store.runners()).isEmpty();
    }

    @Test
    void aRunnerInTryWithResourcesIsStoppedAtTheEndOfTheBlock() {
        BackgroundRunner seen;
        try (var runner = BackgroundRunner.builder()
                .store(new InMemoryStore(new MutableClock()))
                .build()) {
            runner.registry().register(TaskType.of("work"), (context, payload) -> {});
            runner.start();
            seen = runner;
        }

        assertThat(seen.health().status()).isEqualTo(RunnerHealth.Status.STOPPED);
    }

    private static void awaitStatus(BackgroundRunner runner, RunnerHealth.Status status) throws InterruptedException {
        var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (runner.health().status() != status && System.nanoTime() < deadline) Thread.sleep(5);
        assertThat(runner.health().status()).isEqualTo(status);
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

        assertThat(store.runners().getFirst().accepting()).isFalse();
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
