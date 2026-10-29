package today.bonfire.oss.bth4j.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;

class QueuesHolderTest {

    private static QueuesHolder holder() {
        return new QueuesHolder("NS", Set.of("NS:Q:A", "NS:Q:B"), List.of("NS:Q:B", "NS:Q:A"), "NS:Q:A");
    }

    @Test
    void validQueueNameIsResolvedWithNamespace() {
        assertThat(holder().getValidQueue("Q:B")).isEqualTo("NS:Q:B");
    }

    @Test
    void blankQueueNameResolvesToDefaultQueue() {
        var h = holder();
        assertThat(h.getValidQueue(null)).isEqualTo("NS:Q:A");
        assertThat(h.getValidQueue("")).isEqualTo("NS:Q:A");
        assertThat(h.getValidQueue("   ")).isEqualTo("NS:Q:A");
    }

    @Test
    void unknownQueueNameIsRejected() {
        assertThatThrownBy(() -> holder().getValidQueue("Q:Z"))
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("NS:Q:Z");
    }

    @Test
    void queueNameWithNamespaceAlreadyAppliedIsRejected() {
        // names come from user space and never carry the namespace
        assertThatThrownBy(() -> holder().getValidQueue("NS:Q:A")).isInstanceOf(TaskErrorException.class);
    }

    @Test
    void allAvailableQueuesAreProcessedInitially() {
        assertThat(holder().queueProcessingStatus)
                .containsEntry("NS:Q:A", true)
                .containsEntry("NS:Q:B", true)
                .hasSize(2);
    }

    @Test
    void queueCollectionsAreCopiedAndUnmodifiable() {
        var available = new HashSet<>(Set.of("NS:Q:A", "NS:Q:B"));
        var toProcess = new ArrayList<>(List.of("NS:Q:A"));
        var h = new QueuesHolder("NS", available, toProcess, "NS:Q:A");

        available.add("NS:Q:C");
        toProcess.add("NS:Q:B");

        assertThat(h.availableQueues).containsExactlyInAnyOrder("NS:Q:A", "NS:Q:B");
        assertThat(h.queuesToProcess).containsExactly("NS:Q:A");
        assertThatThrownBy(() -> h.availableQueues.add("x")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> h.queuesToProcess.add("x")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void queueOrderOfQueuesToProcessIsKept() {
        assertThat(holder().queuesToProcess).containsExactly("NS:Q:B", "NS:Q:A");
    }
}
