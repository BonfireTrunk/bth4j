package today.bonfire.oss.bth4j.redis;

import java.time.Duration;
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
    protected FullStore newStore(String namespace) {
        return new RedisTaskStore(jedis, namespace);
    }
}
