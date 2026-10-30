package today.bonfire.oss.bth4j;

import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import today.bonfire.oss.bth4j.redis.RedisTaskStore;
import today.bonfire.oss.bth4j.store.TaskStore;

/** {@link ThroughputContract} against a real Redis. Needs Docker. */
@Testcontainers
class RedisThroughputIT extends ThroughputContract {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private RedisClient jedis;

    @Override
    protected synchronized TaskStore newStore(String namespace) {
        if (jedis == null) {
            jedis = RedisClient.builder()
                    .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                    .clientConfig(DefaultJedisClientConfig.builder()
                            .resp3()
                            .timeoutMillis(2000)
                            .build())
                    .poolConfig(ConnectionPoolConfig.builder()
                            .maxPoolSize(64)
                            .waitingForObjectTimeout(Duration.ofSeconds(5))
                            .build())
                    .build();
        }
        return new RedisTaskStore(jedis, namespace);
    }

    @Override
    @AfterAll
    protected void afterAll() {
        super.afterAll();
        if (jedis != null) {
            jedis.close();
        }
    }
}
