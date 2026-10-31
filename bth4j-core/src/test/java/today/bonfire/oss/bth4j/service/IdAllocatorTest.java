package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.TaskStore;

@Timeout(30)
class IdAllocatorTest {

    private static final class CountingStore implements TaskStore {
        final List<Integer> blockSizes = Collections.synchronizedList(new ArrayList<>());
        private long next = 100;

        @Override
        public synchronized long allocateIds(int count) {
            blockSizes.add(count);
            long first = next;
            next += count;
            return first;
        }

        @Override
        public StoreInfo info() {
            return new StoreInfo("test", StoreInfo.SCHEMA_VERSION);
        }

        @Override
        public void init() {}

        @Override
        public int allocateWorkerId(String label) {
            return 1;
        }

        @Override
        public int queueId(String name) {
            return 1;
        }

        @Override
        public int eventId(String name) {
            return 1;
        }

        @Override
        public Map<Integer, String> queueNames() {
            return Map.of();
        }

        @Override
        public Map<Integer, String> eventNames() {
            return Map.of();
        }
    }

    @Test
    void idsIncreaseAndOneStoreCallServesAWholeBlock() {
        var store = new CountingStore();
        var allocator = new IdAllocator(store, 4);

        var ids = new ArrayList<Long>();
        for (int i = 0; i < 4; i++) ids.add(allocator.nextId());

        assertThat(ids).containsExactly(100L, 101L, 102L, 103L);
        assertThat(store.blockSizes).containsExactly(4);
    }

    @Test
    void nextBlockIsReservedWhenTheCurrentOneIsUsedUp() {
        var store = new CountingStore();
        var allocator = new IdAllocator(store, 3);

        var ids = new ArrayList<Long>();
        for (int i = 0; i < 7; i++) ids.add(allocator.nextId());

        assertThat(ids).containsExactly(100L, 101L, 102L, 103L, 104L, 105L, 106L);
        assertThat(store.blockSizes).containsExactly(3, 3, 3);
    }

    @Test
    void blockSizeBelowOneIsTreatedAsOne() {
        var store = new CountingStore();
        var allocator = new IdAllocator(store, 0);

        assertThat(allocator.nextId()).isEqualTo(100L);
        assertThat(allocator.nextId()).isEqualTo(101L);
        assertThat(store.blockSizes).containsExactly(1, 1);
    }

    @Test
    void idsAreUniqueAcrossThreads() throws Exception {
        var store = new CountingStore();
        var allocator = new IdAllocator(store, 8);
        var seen = Collections.synchronizedSet(new HashSet<Long>());
        int threads = 8;
        int perThread = 500;
        var done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            Thread.ofVirtual().start(() -> {
                for (int i = 0; i < perThread; i++) seen.add(allocator.nextId());
                done.countDown();
            });
        }

        assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(seen).hasSize(threads * perThread);
    }
}
