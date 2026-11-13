package today.bonfire.oss.bth4j.redis;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import today.bonfire.oss.bth4j.RunnerFaultContract;
import today.bonfire.oss.bth4j.fault.Faulty;

/** {@link RunnerFaultContract} against {@link RedisTaskStore} on a real Redis or Valkey. Needs Docker. */
@Testcontainers
class RedisRunnerFaultIT extends RunnerFaultContract {

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
}
