package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KeysTest {

    private final Keys keys = new Keys("NS");

    @Test
    void everyKeyCarriesNamespaceAsHashTag() {
        assertThat(keys.scheduled).startsWith("{NS}:");
        assertThat(keys.leased).startsWith("{NS}:");
        assertThat(keys.dead).startsWith("{NS}:");
        assertThat(keys.cron).startsWith("{NS}:");
        assertThat(keys.wakeChannel).startsWith("{NS}:");
        assertThat(keys.taskPrefix).startsWith("{NS}:");
        assertThat(keys.queuePrefix).startsWith("{NS}:");
        assertThat(keys.cronPrefix).startsWith("{NS}:");
        assertThat(keys.task("id")).startsWith("{NS}:");
        assertThat(keys.queue("q")).startsWith("{NS}:");
        assertThat(keys.cronDefinition("s")).startsWith("{NS}:");
    }

    @Test
    void composedKeysUsePrefixes() {
        assertThat(keys.task("id")).isEqualTo(keys.taskPrefix + "id");
        assertThat(keys.queue("q")).isEqualTo(keys.queuePrefix + "q");
        assertThat(keys.cronDefinition("s")).isEqualTo(keys.cronPrefix + "s");
    }

    @Test
    void namespaceIsKept() {
        assertThat(keys.namespace).isEqualTo("NS");
    }

    @Test
    void fixedKeysAreDistinct() {
        assertThat(java.util.Set.of(
                        keys.scheduled,
                        keys.leased,
                        keys.dead,
                        keys.cron,
                        keys.wakeChannel,
                        keys.taskPrefix,
                        keys.queuePrefix,
                        keys.cronPrefix))
                .hasSize(8);
    }
}
