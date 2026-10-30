package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

class RedisTaskStoreTest {

    private static final RedisClient CLIENT =
            RedisClient.builder().hostAndPort(new HostAndPort("localhost", 1)).build();

    @AfterAll
    static void closeClient() {
        CLIENT.close();
    }

    @Test
    void nullClientIsRejected() {
        assertThatThrownBy(() -> new RedisTaskStore(null, "ok")).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void blankNamespaceIsRejected() {
        assertThatThrownBy(() -> new RedisTaskStore(CLIENT, " ")).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void namespaceWithBracesIsRejected() {
        assertThatThrownBy(() -> new RedisTaskStore(CLIENT, "a{b")).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> new RedisTaskStore(CLIENT, "a}b")).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void plainNamespaceIsAccepted() {
        assertThatCode(() -> new RedisTaskStore(CLIENT, "ok")).doesNotThrowAnyException();
    }
}
