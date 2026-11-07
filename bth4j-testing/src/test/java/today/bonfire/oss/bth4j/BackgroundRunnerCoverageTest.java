package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
    void quietAndResumePublishOnTheMaintenanceThreadWithoutWaitingForTheStore() throws Exception {
        var store = new InMemoryStore(new MutableClock());
        var heartbeats = new LinkedBlockingQueue<Boolean>();
        var quietHeartbeatEntered = new CountDownLatch(1);
        var releaseQuietHeartbeat = new CountDownLatch(1);
        var caller = Thread.currentThread();
        var heartbeatThreads = new LinkedBlockingQueue<Thread>();
        var observed = StoreInterceptor.around(store, (method, args, real) -> {
            if (!method.equals("heartbeat")) return real.proceed();
            boolean accepting = (boolean) args[2];
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
            if (method.equals("heartbeat")) published.add((boolean) args[2]);
            return result;
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .maintenanceCheckInterval(Duration.ofHours(1))
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
            if (method.equals("heartbeat")) published.add((boolean) args[2]);
            return result;
        });
        var runner = BackgroundRunner.builder()
                .store(observed)
                .maintenanceCheckInterval(Duration.ofHours(1))
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
}
