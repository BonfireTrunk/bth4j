package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            return new StoreInfo("test", 1);
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
        public int typeId(String name) {
            return 1;
        }

        @Override
        public Map<Integer, String> queueNames() {
            return Map.of();
        }

        @Override
        public Map<Integer, String> typeNames() {
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
    void nextIdsServesARangeFromTheCurrentBlockWhenItHasRoom() {
        var store = new CountingStore();
        var allocator = new IdAllocator(store, 10);
        assertThat(allocator.nextId()).isEqualTo(100L);

        assertThat(allocator.nextIds(4)).isEqualTo(101L);

        assertThat(allocator.nextId()).isEqualTo(105L);
        assertThat(store.blockSizes).containsExactly(10);
    }

    @Test
    void nextIdsReservesOneBlockOfAtLeastTheCountAndAbandonsTheRest() {
        var store = new CountingStore();
        var allocator = new IdAllocator(store, 4);
        assertThat(allocator.nextId()).isEqualTo(100L);

        assertThat(allocator.nextIds(7)).isEqualTo(104L);
        assertThat(allocator.nextIds(2)).isEqualTo(111L);

        assertThat(allocator.nextId()).isEqualTo(113L);
        assertThat(store.blockSizes).containsExactly(4, 7, 4);
        assertThatThrownBy(() -> allocator.nextIds(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void abandoningABlockPreservesTheKnownEndAndReservesAFreshBlock() {
        var store = new CountingStore();
        var allocator = new IdAllocator(store, 10);
        assertThat(allocator.knownEnd()).isZero();
        assertThat(allocator.nextId()).isEqualTo(100);
        assertThat(allocator.knownEnd()).isEqualTo(110);

        allocator.abandonBlock();

        assertThat(allocator.knownEnd()).isEqualTo(110);
        assertThat(allocator.nextIds(2)).isEqualTo(110);
        assertThat(allocator.nextId()).isEqualTo(112);
        assertThat(allocator.knownEnd()).isEqualTo(120);
        assertThat(store.blockSizes).containsExactly(10, 10);
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
