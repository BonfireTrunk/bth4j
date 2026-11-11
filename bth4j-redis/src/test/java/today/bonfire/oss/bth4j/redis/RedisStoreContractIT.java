package today.bonfire.oss.bth4j.redis;

import java.time.Duration;
import java.util.Comparator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.StoreContract;
import today.bonfire.oss.bth4j.spi.TaskStore;

/** The shared store contract run against {@link RedisTaskStore} on a real Redis. Needs Docker. */
@Testcontainers
class RedisStoreContractIT extends StoreContract {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(RedisImage.NAME)
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static RedisClient jedis;

    @BeforeAll
    static void connect() {
        var poolConfig = ConnectionPoolConfig.builder()
                .maxPoolSize(16)
                .minPoolSize(1)
                .waitingForObjectTimeout(Duration.ofSeconds(5))
                .build();
        var clientConfig = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(3000)
                .clientName("bth4j-store-contract-test")
                .build();
        jedis = RedisClient.builder()
                .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                .clientConfig(clientConfig)
                .poolConfig(poolConfig)
                .build();
    }

    @AfterAll
    static void disconnect() {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    protected boolean releasesKeysOnItsOwn() {
        return true;
    }

    @Override
    protected Comparator<Long> orderOfIdsThatShareATime() {
        return Comparator.comparing(String::valueOf);
    }

    @Override
    protected FullStore newStore(String namespace) {
        return new RedisTaskStore(jedis, namespace);
    }

    @Override
    protected boolean corruptStoredValue(String namespace, long taskId) {
        jedis.hset(new Keys(namespace).task(taskId), Keys.F_HEADERS, "{\"a\":null}");
        return true;
    }

    @Override
    protected boolean writeOtherSchemaVersion(String namespace) {
        jedis.hset(new Keys(namespace).meta, Keys.SCHEMA, String.valueOf(RedisTaskStore.LAYOUT_VERSION + 1));
        return true;
    }

    @Override
    protected void fillRegistry(TaskStore fresh, String namespace, Registry registry, int lastId) {
        var counter = new Keys(namespace).prefix + "seq:" + (registry == Registry.QUEUE ? "queue" : "type");
        jedis.set(counter, Integer.toString(lastId));
    }
}
