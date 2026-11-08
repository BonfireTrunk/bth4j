package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.spi.ExecutionStore;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

@Timeout(30)
class AckBatcherTest {

    private final List<List<LeaseRef>> batches = Collections.synchronizedList(new ArrayList<>());

    /** A store that answers only ackAll, through the given function, and records every batch. */
    private ExecutionStore store(Function<List<LeaseRef>, List<Boolean>> ackAll) {
        return (ExecutionStore) Proxy.newProxyInstance(
                ExecutionStore.class.getClassLoader(), new Class<?>[] {ExecutionStore.class}, (proxy, m, args) -> {
                    if (!m.getName().equals("ackAll")) throw new UnsupportedOperationException(m.getName());
                    @SuppressWarnings("unchecked")
                    var leases = (List<LeaseRef>) args[0];
                    batches.add(leases);
                    return ackAll.apply(leases);
                });
    }

    private static List<Boolean> allApplied(List<LeaseRef> leases) {
        return leases.stream().map(l -> true).toList();
    }

    @Test
    void aQuietRunnerSendsEachAcknowledgementAtOnce() {
        var batcher = new AckBatcher(store(AckBatcherTest::allApplied), true, false);

        assertThat(batcher.ack(1, 3)).isTrue();
        assertThat(batcher.ack(2, 5)).isTrue();

        assertThat(batches).containsExactly(List.of(new LeaseRef(1, 3)), List.of(new LeaseRef(2, 5)));
    }

    @Test
    void acknowledgementsThatArriveWhileABatchIsOnItsWayGoTogetherInTheNext() throws Exception {
        var firstSent = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var batcher = new AckBatcher(
                store(leases -> {
                    if (leases.getFirst().id() == 1) {
                        firstSent.countDown();
                        await(release);
                    }
                    return leases.stream().map(l -> l.id() != 3).toList();
                }),
                true,
                false);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = pool.submit(() -> batcher.ack(1, 1));
            firstSent.await();
            var later = new ArrayList<Future<Boolean>>();
            for (var id = 2; id <= 4; id++) {
                var task = id;
                later.add(pool.submit(() -> batcher.ack(task, 1)));
            }
            while (waiting(batcher) < 3) Thread.onSpinWait();
            release.countDown();

            assertThat(first.get()).isTrue();
            assertThat(later.get(0).get()).isTrue();
            assertThat(later.get(1).get()).isFalse();
            assertThat(later.get(2).get()).isTrue();
        }
        assertThat(batches).hasSize(2);
        assertThat(batches.get(1)).extracting(LeaseRef::id).containsExactlyInAnyOrder(2L, 3L, 4L);
    }

    @Test
    void manyConcurrentAcknowledgementsEachGetTheirOwnResultAndGoOutOnce() throws Exception {
        var batcher = new AckBatcher(
                store(leases -> {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return leases.stream().map(l -> l.id() % 7 != 0).toList();
                }),
                true,
                false);
        var results = new ArrayList<Future<Boolean>>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var id = 1; id <= 2_000; id++) {
                var task = id;
                results.add(pool.submit(() -> batcher.ack(task, 1)));
            }
            for (var id = 1; id <= 2_000; id++) {
                assertThat(results.get(id - 1).get()).as("ack %d", id).isEqualTo(id % 7 != 0);
            }
        }
        assertThat(batches.stream().flatMap(List::stream).map(LeaseRef::id))
                .hasSize(2_000)
                .doesNotHaveDuplicates();
        assertThat(batches.size()).isLessThan(2_000);
        assertThat(batcher.waitingCount()).isZero();
    }

    @Test
    void aFailedBatchCallLeavesEveryAcknowledgementInItUnknown() throws Exception {
        var firstSent = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var batcher = new AckBatcher(
                store(leases -> {
                    if (leases.getFirst().id() == 1) {
                        firstSent.countDown();
                        await(release);
                        return allApplied(leases);
                    }
                    throw new TaskStoreException("connection reset");
                }),
                true,
                false);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = pool.submit(() -> batcher.ack(1, 1));
            firstSent.await();
            var second = pool.submit(() -> batcher.ack(2, 1));
            var third = pool.submit(() -> batcher.ack(3, 1));
            while (waiting(batcher) < 2) Thread.onSpinWait();
            release.countDown();

            assertThat(first.get()).isTrue();
            for (var f : List.of(second, third)) {
                assertThatThrownBy(f::get)
                        .isInstanceOf(ExecutionException.class)
                        .cause()
                        .isInstanceOf(TaskStoreException.class)
                        .hasMessage("connection reset");
            }
        }
        assertThatThrownBy(() -> batcher.ack(4, 1)).hasMessage("connection reset");
    }

    @Test
    void onlyTheSenderRethrowsAnErrorAsItself() throws Exception {
        var firstSent = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var error = new StackOverflowError("deep");
        var batcher = new AckBatcher(
                store(leases -> {
                    if (leases.getFirst().id() == 1) {
                        firstSent.countDown();
                        await(release);
                        return allApplied(leases);
                    }
                    throw error;
                }),
                true,
                false);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            pool.submit(() -> batcher.ack(1, 1));
            firstSent.await();
            var second = pool.submit(() -> batcher.ack(2, 1));
            var third = pool.submit(() -> batcher.ack(3, 1));
            while (waiting(batcher) < 2) Thread.onSpinWait();
            release.countDown();

            var outcomes = new ArrayList<Throwable>();
            for (var f : List.of(second, third)) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    outcomes.add(e.getCause());
                }
            }
            assertThat(outcomes).filteredOn(t -> t == error).hasSize(1);
            assertThat(outcomes)
                    .filteredOn(t -> t != error)
                    .singleElement()
                    .isInstanceOf(TaskStoreException.class)
                    .extracting(Throwable::getCause)
                    .isSameAs(error);
        }
    }

    @Test
    void aStoreAnsweringTheWrongNumberOfAcknowledgementsIsAFailure() {
        var batcher = new AckBatcher(store(leases -> List.of()), true, false);

        assertThatThrownBy(() -> batcher.ack(1, 1))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("answered 0 acknowledgements for 1");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int waiting(AckBatcher batcher) {
        return batcher.waitingCount();
    }
}
