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

/** {@link BackgroundRunnerContract} against a real Redis. Needs Docker. */
@Testcontainers
class RedisBackgroundRunnerIT extends BackgroundRunnerContract {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private RedisClient jedis;

    @Override
    protected synchronized TaskStore newStore(String namespace) {
        if (jedis == null) {
            var poolConfig = ConnectionPoolConfig.builder()
                    .maxPoolSize(16)
                    .minPoolSize(1)
                    .testOnBorrow(false)
                    .testWhileIdle(true)
                    .testOnReturn(false)
                    .objEvictionTimeout(Duration.ofSeconds(120))
                    .durationBetweenEvictionsRuns(Duration.ofSeconds(5))
                    .waitingForObjectTimeout(Duration.ofSeconds(2))
                    .abandonedTimeout(Duration.ofSeconds(120))
                    .durationBetweenAbandonCheckRuns(Duration.ofSeconds(2))
                    .build();
            var clientConfig = DefaultJedisClientConfig.builder()
                    .resp3()
                    .timeoutMillis(1000)
                    .clientName("bth4j-test")
                    .build();
            jedis = RedisClient.builder()
                    .hostAndPort(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)))
                    .clientConfig(clientConfig)
                    .poolConfig(poolConfig)
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
