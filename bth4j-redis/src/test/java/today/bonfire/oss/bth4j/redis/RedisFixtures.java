package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/** Redis test fixtures: task history keys, checks that every trace of a task is gone, and aging of due times. */
final class RedisFixtures {

    private static final Map<String, StepResult> STEPS =
            Map.of("a", new StepResult(Instant.ofEpochMilli(1_800_000_000_000L), "{\"x\":1}"));

    private RedisFixtures() {}

    /**
     * Saves steps on a leased task and fails it for a retry at the given time, so it waits again with a step key
     * and a failure list. A payload key exists when the task was enqueued with a payload.
     */
    static void addHistory(RedisTaskStore store, TaskRecord leased, Instant retryAt) {
        assertThat(store.saveSteps(leased.id(), leased.revision(), STEPS)).isTrue();
        var failure = new FailureInfo(1, Instant.now().truncatedTo(ChronoUnit.MILLIS), "java.lang.Boom", "boom", null);
        assertThat(store.fail(leased.id(), leased.revision(), failure, retryAt, true, null))
                .isEqualTo(FailOutcome.RETRY);
    }

    /** Adds the history, moves the task back to its queue and leases it again. */
    static TaskRecord leasedWithHistory(RedisTaskStore store, int queue, TaskRecord leased) {
        addHistory(store, leased, Instant.now().minusSeconds(5));
        assertThat(store.promote(10).moved()).isEqualTo(1);
        return store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 1, 1)), 1, Duration.ofSeconds(30), 1, null))
                .getFirst();
    }

    /** Moves the due time of a scheduled task, in its hash and in the scheduled zset, to the given instant. */
    static void ageDue(UnifiedJedis jedis, Keys keys, int queue, long id, Instant due) {
        jedis.zadd(keys.scheduled(queue), due.toEpochMilli(), String.valueOf(id));
        jedis.hset(keys.task(id), "due", String.valueOf(due.toEpochMilli()));
    }

    static void assertPopulated(UnifiedJedis jedis, Keys keys, long id) {
        assertThat(jedis.exists(keys.task(id))).as("hash of %d", id).isTrue();
        assertThat(jedis.exists(keys.data(id))).as("payload of %d", id).isTrue();
        assertThat(jedis.exists(keys.steps(id))).as("steps of %d", id).isTrue();
        assertThat(jedis.exists(keys.failures(id))).as("failures of %d", id).isTrue();
    }

    static void assertGone(UnifiedJedis jedis, Keys keys, long id) {
        assertThat(jedis.exists(keys.task(id), keys.data(id), keys.steps(id), keys.failures(id)))
                .as("keys of %d", id)
                .isZero();
        var member = String.valueOf(id);
        for (var key : jedis.keys(keys.prefix + "*")) {
            if ("zset".equals(jedis.type(key))) {
                assertThat(jedis.zscore(key, member)).as("%s holds %d", key, id).isNull();
            }
        }
    }

    /**
     * Checks the layout of the queued tasks of a namespace: for every queue, {@code qa:<qid>} holds, with the same
     * scores, exactly the ids of all {@code q:<qid>:*} sets (found by SCAN, not through {@code active:<qid>}), no id
     * is in two type sets, and {@code active:<qid>} names exactly the types whose set is not empty. The queues are
     * the registered ones and every queue that has one of these keys.
     */
    static void assertQueueSetsAgree(UnifiedJedis jedis, String namespace) {
        var keys = new Keys(namespace);
        var queues = new HashSet<>(jedis.hvals(keys.registry("queue")));
        var perType = new TreeMap<String, Map<String, Map<String, Double>>>();
        for (var key : scan(jedis, keys.prefix + "q:*")) {
            var parts = key.substring((keys.prefix + "q:").length()).split(":");
            var members = new HashMap<String, Double>();
            for (var t : jedis.zrangeWithScores(key, 0, -1)) members.put(t.getElement(), t.getScore());
            perType.computeIfAbsent(parts[0], q -> new TreeMap<>()).put(parts[1], members);
        }
        for (var key : scan(jedis, keys.prefix + "qa:*")) {
            perType.computeIfAbsent(key.substring((keys.prefix + "qa:").length()), q -> new TreeMap<>());
        }
        for (var key : scan(jedis, keys.prefix + "active:*")) {
            perType.computeIfAbsent(key.substring((keys.prefix + "active:").length()), q -> new TreeMap<>());
        }
        for (var queue : queues) perType.computeIfAbsent(queue, q -> new TreeMap<>());
        for (var entry : perType.entrySet()) {
            var qid = Integer.parseInt(entry.getKey());
            var union = new HashMap<String, Double>();
            var nonEmpty = new HashSet<String>();
            for (var type : entry.getValue().entrySet()) {
                if (!type.getValue().isEmpty()) nonEmpty.add(type.getKey());
                for (var member : type.getValue().entrySet()) {
                    assertThat(union.put(member.getKey(), member.getValue()))
                            .as("%s queue %d: task %s in two type sets", namespace, qid, member.getKey())
                            .isNull();
                }
            }
            var all = new HashMap<String, Double>();
            for (var t : jedis.zrangeWithScores(keys.queueAll(qid), 0, -1)) all.put(t.getElement(), t.getScore());
            assertThat(all)
                    .as("%s: qa of queue %d against the union of its type sets", namespace, qid)
                    .isEqualTo(union);
            assertThat(jedis.smembers(keys.activeTypes(qid)))
                    .as("%s: active types of queue %d", namespace, qid)
                    .isEqualTo((Set<String>) nonEmpty);
        }
    }

    private static List<String> scan(UnifiedJedis jedis, String pattern) {
        var found = new java.util.ArrayList<String>();
        var cursor = ScanParams.SCAN_POINTER_START;
        do {
            var page = jedis.scan(cursor, new ScanParams().match(pattern).count(1000));
            found.addAll(page.getResult());
            cursor = page.getCursor();
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        return found;
    }

    /** Remembers the namespaces a test used, so that one after-each hook can check each of them. */
    static final class UsedNamespaces {
        private final Set<String> used = java.util.concurrent.ConcurrentHashMap.newKeySet();

        void add(String namespace) {
            used.add(namespace);
        }

        void forget(String namespace) {
            used.remove(namespace);
        }

        void assertQueueSetsAgree(UnifiedJedis jedis) {
            var all = new java.util.TreeSet<>(used);
            used.clear();
            for (var namespace : all) RedisFixtures.assertQueueSetsAgree(jedis, namespace);
        }
    }
}
