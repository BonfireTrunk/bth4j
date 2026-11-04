package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

class RedisTaskStoreTest {

    @Test
    void enqueueReusesItsTokenBlockAndRefillsAfterTheLastToken() {
        var jedis = new RecordingJedis();
        var store = new RedisTaskStore(jedis, "tokens");
        var size = RedisTaskStore.WRITE_TOKEN_BLOCK_SIZE;
        var first = store.allocateIds(size + 1);
        jedis.reservations.clear();

        for (int i = 0; i < size; i++) {
            assertThat(store.enqueue(task(first + i))).isEqualTo(EnqueueOutcome.STORED);
        }

        assertThat(jedis.reservations).containsExactly((long) size);
        assertThat(jedis.tokens).hasSize(size).doesNotHaveDuplicates();
        assertThat(jedis.tokens.getFirst()).isEqualTo(first + size + 1);
        assertThat(jedis.tokens.getLast()).isEqualTo(first + 2 * size);
        assertThat(store.enqueue(task(first + size))).isEqualTo(EnqueueOutcome.STORED);
        assertThat(jedis.reservations).containsExactly((long) size, (long) size);
        assertThat(jedis.tokens.getLast()).isEqualTo(first + 2 * size + 1);
    }

    @Test
    void reservationFailurePreventsSingleAndBatchWritesAndAllowsRecovery() {
        var jedis = new RecordingJedis();
        var store = new RedisTaskStore(jedis, "tokens");
        var first = store.allocateIds(1);
        jedis.failReservation = true;

        assertThatThrownBy(() -> store.enqueue(task(first)))
                .isInstanceOf(TaskStoreException.class)
                .hasCauseInstanceOf(JedisException.class);
        assertThatThrownBy(() -> store.enqueueAll(List.of(task(first))))
                .isInstanceOf(TaskStoreException.class)
                .hasCauseInstanceOf(JedisException.class);
        assertThat(store.enqueueAll(List.of())).isEmpty();
        assertThat(jedis.tokens).isEmpty();

        jedis.failReservation = false;
        assertThat(store.enqueue(task(first))).isEqualTo(EnqueueOutcome.STORED);
        assertThat(jedis.tokens).containsExactly(first + 1);
    }

    @Test
    void tokenBlockOverflowFailsWithoutWritingOrReusingTokens() {
        var jedis = new RecordingJedis();
        var store = new RedisTaskStore(jedis, "tokens");
        jedis.last = Long.MAX_VALUE - RedisTaskStore.WRITE_TOKEN_BLOCK_SIZE;

        assertThatThrownBy(() -> store.enqueue(task(1)))
                .isInstanceOf(TaskStoreException.class)
                .hasCauseInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> store.enqueue(task(1)))
                .isInstanceOf(TaskStoreException.class)
                .hasCauseInstanceOf(JedisException.class);
        assertThat(jedis.tokens).isEmpty();
    }

    private static TaskRecord task(long id) {
        return TaskRecord.builder().id(id).queueId(1).eventId(1).build();
    }

    private static final class RecordingJedis extends UnifiedJedis {
        private final List<Long> reservations = new ArrayList<>();
        private final List<Long> tokens = new ArrayList<>();
        private long last;
        private boolean failReservation;

        private RecordingJedis() {
            super(null, (RedisProtocol) null);
        }

        @Override
        public long incrBy(String key, long count) {
            if (failReservation || last > Long.MAX_VALUE - count) {
                throw new JedisException("reservation failed");
            }
            reservations.add(count);
            last += count;
            return last;
        }

        @Override
        public Object evalsha(String sha, List<String> keys, List<String> args) {
            tokens.add(Long.parseLong(args.getLast()));
            return List.of(1L, "");
        }
    }
}
