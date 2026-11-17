package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

class RedisTaskStoreBuilderTest {

    @Test
    void clientIsRequired() {
        assertThatThrownBy(() -> RedisTaskStore.builder().namespace("ok").build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessage("Jedis client is required");
    }

    @Test
    void namespaceIsRequiredAndMustNotBeBlankOrContainBraces() {
        try (var client = unconnectedClient()) {
            for (var bad : new String[] {null, "", " ", "a{b", "a}b"}) {
                assertThatThrownBy(() -> RedisTaskStore.builder()
                                .client(client)
                                .namespace(bad)
                                .build())
                        .as("namespace %s", bad)
                        .isInstanceOf(TaskConfigurationException.class)
                        .hasMessage("namespace cannot be blank or contain braces");
            }
        }
    }

    @Test
    void oneBuilderBuildsTwoStoresThatDoNotShareItsLaterValues() {
        try (var client = unconnectedClient()) {
            var builder = RedisTaskStore.builder().client(client).namespace("first");
            var first = builder.build();
            var second = builder.namespace("second").build();

            assertThat(first.keys().namespace).isEqualTo("first");
            assertThat(second.keys().namespace).isEqualTo("second");
        }
    }

    private static RedisClient unconnectedClient() {
        return RedisClient.builder()
                .hostAndPort(new HostAndPort("localhost", 1))
                .build();
    }
}
