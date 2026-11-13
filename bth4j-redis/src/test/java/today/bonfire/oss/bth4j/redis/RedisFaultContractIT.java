package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FaultContract;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * {@link FaultContract} against {@link RedisTaskStore} on a real Redis or Valkey. The store under test reaches the
 * server through {@link FaultyRedisProxy}, which cuts the connection at a command: a fault point is one command of
 * the client, so a pipeline has one point per command. Needs Docker.
 */
@Testcontainers
class RedisFaultContractIT extends FaultContract {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(RedisImage.NAME)
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static RedisFaults faults;

    @BeforeAll
    static void connect() {
        faults = new RedisFaults(REDIS.getHost(), REDIS.getMappedPort(6379));
    }

    @AfterAll
    static void disconnect() {
        faults.close();
    }

    @Override
    protected Faulty newFaulty(String namespace) {
        return faults.faulty(namespace);
    }

    // a driver that repeats a command whose connection failed (Jedis does so only when asked to)

    private static final AtomicInteger RETRY_NAMESPACES = new AtomicInteger();

    private record Retrying(Faulty faulty, int queue, int type) {
        Retrying() {
            this(RedisFaultContractIT.retrying());
        }

        Retrying(Faulty faulty) {
            this(faulty, faulty.plain().queueId("main"), faulty.plain().typeId("work"));
        }

        TaskRecord.Builder task() {
            return TaskRecord.builder()
                    .id(faulty.plain().allocateIds(1))
                    .queueId(queue)
                    .typeId(type)
                    .maxAttempts(3)
                    .createdAt(Instant.now())
                    .encodedPayload("payload");
        }

        List<TaskRecord> claim(int count) {
            return faulty.plain()
                    .claim(new ClaimRequest(
                            List.of(new QueueGrant(queue, count, count)), count, Duration.ofSeconds(1), 1, null));
        }
    }

    private static Faulty retrying() {
        var f = faults.faultyRetrying("retry" + RETRY_NAMESPACES.incrementAndGet() + "x" + System.nanoTime() % 100_000);
        f.plain().init();
        return f;
    }

    @Test
    void anEnqueueTheDriverRepeatsAfterALostReplyAnswersStoredAndStoresOneTask() {
        var r = new Retrying();
        var record = r.task().build();

        r.faulty().plan().cutAt(1, FaultPlan.Cut.AFTER);
        var first = r.faulty().store().enqueue(record);
        r.faulty().plan().disarm();

        assertThat(r.faulty().plan().fired()).isTrue();
        assertThat(first)
                .as("the repeated command carries the same write token")
                .isEqualTo(EnqueueOutcome.STORED);
        assertThat(r.faulty().plain().counts().get(r.queue()).queued()).isEqualTo(1);
        assertThat(r.faulty().store().enqueue(record))
                .as("a new call of the caller")
                .isEqualTo(EnqueueOutcome.EXISTS);
    }

    @Test
    void anEnqueueTheDriverRepeatsAfterARequestThatNeverArrivedStoresOneTask() {
        var r = new Retrying();
        var record = r.task().build();

        r.faulty().plan().cutAt(1, FaultPlan.Cut.BEFORE);
        var first = r.faulty().store().enqueue(record);
        r.faulty().plan().disarm();

        assertThat(r.faulty().plan().fired()).isTrue();
        assertThat(first).isEqualTo(EnqueueOutcome.STORED);
        assertThat(r.faulty().plain().counts().get(r.queue()).queued()).isEqualTo(1);
    }

    @Test
    void anAckTheDriverRepeatsAfterALostReplyAnswersFalseThoughItApplied() {
        var r = new Retrying();
        r.faulty().plain().enqueue(r.task().build());
        var leased = r.claim(1).getFirst();

        r.faulty().plan().cutAt(1, FaultPlan.Cut.AFTER);
        var acked = r.faulty().store().ack(leased.id(), leased.revision(), true, true, null);
        r.faulty().plan().disarm();

        assertThat(r.faulty().plan().fired()).isTrue();
        assertThat(acked).as("the second attempt finds the revision moved on").isFalse();
        assertThat(r.faulty().plain().task(leased.id()).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
    }

    @Test
    void aClaimTheDriverRepeatsAfterALostReplyLeavesTheFirstClaimLeasedAndUnseen() {
        var r = new Retrying();
        var first = r.task().build();
        var second = r.task().build();
        r.faulty().plain().enqueue(first);
        r.faulty().plain().enqueue(second);

        r.faulty().plan().cutAt(1, FaultPlan.Cut.AFTER);
        var claimed = r.faulty()
                .store()
                .claim(new ClaimRequest(List.of(new QueueGrant(r.queue(), 1, 1)), 1, Duration.ofSeconds(1), 1, null));
        r.faulty().plan().disarm();

        assertThat(claimed)
                .extracting(TaskRecord::id)
                .as("the repeated claim takes the next task")
                .containsExactly(second.id());
        assertThat(r.faulty().plain().task(first.id()).orElseThrow().state())
                .as("the task of the lost reply is leased and nobody runs it")
                .isEqualTo(TaskState.LEASED);
    }
}
