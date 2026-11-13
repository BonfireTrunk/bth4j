package today.bonfire.oss.bth4j.redis;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.executors.RetryableCommandExecutor;
import redis.clients.jedis.providers.PooledConnectionProvider;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;

/**
 * Stores on a Redis or Valkey server that a {@link FaultPlan} can cut: the store under test reaches the server
 * through {@link FaultyRedisProxy}, a plain store reaches it directly. Jedis runs with its default command
 * executor, which does not repeat a command whose connection failed, so each cut reaches the store once.
 */
final class RedisFaults implements AutoCloseable {
    private final FaultPlan plan = new FaultPlan();
    private final FaultyRedisProxy proxy;
    private final RedisClient direct;
    private final RedisClient viaProxy;
    private final RedisClient retrying;

    RedisFaults(String host, int port) {
        try {
            proxy = new FaultyRedisProxy(host, port, plan);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        direct = client(host, port);
        viaProxy = client("127.0.0.1", proxy.port());
        retrying = retryingClient("127.0.0.1", proxy.port());
    }

    /**
     * A client whose command executor repeats a command when its connection fails, three attempts in all, which
     * Jedis does only when the application asks for it. A pipeline is not repeated.
     */
    private static RedisClient retryingClient(String host, int port) {
        var poolConfig = ConnectionPoolConfig.builder()
                .maxPoolSize(16)
                .minPoolSize(1)
                .waitingForObjectTimeout(Duration.ofSeconds(5))
                .build();
        var clientConfig = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(3000)
                .clientName("bth4j-fault-test-retrying")
                .build();
        var provider = new PooledConnectionProvider(new HostAndPort(host, port), clientConfig, poolConfig);
        return RedisClient.builder()
                .clientConfig(clientConfig)
                .connectionProvider(provider)
                .commandExecutor(new RetryableCommandExecutor(provider, 3, Duration.ofSeconds(10)))
                .build();
    }

    private static RedisClient client(String host, int port) {
        var poolConfig = ConnectionPoolConfig.builder()
                .maxPoolSize(16)
                .minPoolSize(1)
                .waitingForObjectTimeout(Duration.ofSeconds(5))
                .build();
        var clientConfig = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(3000)
                .clientName("bth4j-fault-test")
                .build();
        return RedisClient.builder()
                .hostAndPort(new HostAndPort(host, port))
                .clientConfig(clientConfig)
                .poolConfig(poolConfig)
                .build();
    }

    Faulty faulty(String namespace) {
        var plain = RedisTaskStore.builder().client(direct).namespace(namespace).build();
        var store =
                RedisTaskStore.builder().client(viaProxy).namespace(namespace).build();
        return new Faulty(store, plain, plan);
    }

    /** Like {@link #faulty}, with a client that repeats a command whose connection failed. */
    Faulty faultyRetrying(String namespace) {
        var plain = RedisTaskStore.builder().client(direct).namespace(namespace).build();
        var store =
                RedisTaskStore.builder().client(retrying).namespace(namespace).build();
        return new Faulty(store, plain, plan);
    }

    @Override
    public void close() {
        retrying.close();
        viaProxy.close();
        direct.close();
        proxy.close();
    }
}
