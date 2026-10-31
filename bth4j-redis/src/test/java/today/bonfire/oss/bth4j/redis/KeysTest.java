package today.bonfire.oss.bth4j.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class KeysTest {

    private final Keys keys = new Keys("NS");

    @Test
    void everyKeyCarriesNamespaceAsHashTag() {
        var all = List.of(
                keys.prefix,
                keys.meta,
                keys.seqId,
                keys.seqWorker,
                keys.scheduled,
                keys.leased,
                keys.queues,
                keys.keyWindows,
                keys.cron,
                keys.cronRevision,
                keys.task(1),
                keys.queue(2),
                keys.index(3, 2),
                keys.counters(2),
                keys.uniqueKey("k"),
                keys.worker(4),
                keys.cronDefinition("s"),
                keys.registry("queue"),
                keys.registryNames("queue"));

        assertThat(all).allSatisfy(k -> assertThat(k).startsWith("{NS}:"));
    }

    @Test
    void layoutMatchesTheDocumentedNames() {
        assertThat(keys.task(7)).isEqualTo("{NS}:t:7");
        assertThat(keys.queue(2)).isEqualTo("{NS}:q:2");
        assertThat(keys.index(4, 2)).isEqualTo("{NS}:idx:4:2");
        assertThat(keys.counters(2)).isEqualTo("{NS}:c:2");
        assertThat(keys.uniqueKey("k")).isEqualTo("{NS}:k:k");
        assertThat(keys.registryNames("event")).isEqualTo("{NS}:reg:event:names");
    }

    @Test
    void fixedKeysAreDistinct() {
        assertThat(List.of(
                        keys.meta,
                        keys.seqId,
                        keys.seqWorker,
                        keys.scheduled,
                        keys.leased,
                        keys.queues,
                        keys.keyWindows,
                        keys.cron,
                        keys.cronRevision))
                .doesNotHaveDuplicates();
    }
}
