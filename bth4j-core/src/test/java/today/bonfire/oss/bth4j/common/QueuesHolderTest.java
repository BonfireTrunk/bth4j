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
        return new QueuesHolder(Set.of("a", "b"), List.of("b", "a"), "a");
    }

    @Test
    void blankQueueNameReturnsDefault() {
        var h = holder();

        assertThat(h.getValidQueue(null)).isEqualTo("a");
        assertThat(h.getValidQueue("")).isEqualTo("a");
        assertThat(h.getValidQueue("  ")).isEqualTo("a");
    }

    @Test
    void knownQueueNameIsReturnedAsIs() {
        assertThat(holder().getValidQueue("b")).isEqualTo("b");
    }

    @Test
    void unknownQueueNameThrows() {
        assertThatThrownBy(() -> holder().getValidQueue("zzz"))
                .isInstanceOf(TaskErrorException.class)
                .hasMessageContaining("zzz");
    }

    @Test
    void exposesConfiguration() {
        var h = holder();

        assertThat(h.defaultQueue).isEqualTo("a");
        assertThat(h.availableQueues).containsExactlyInAnyOrder("a", "b");
        assertThat(h.queuesToProcess).containsExactly("b", "a");
    }

    @Test
    void collectionsAreUnmodifiable() {
        var h = holder();

        assertThatThrownBy(() -> h.availableQueues.add("c")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> h.queuesToProcess.add("c")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void collectionsAreCopies() {
        var available = new HashSet<>(Set.of("a"));
        var process = new ArrayList<>(List.of("a"));
        var h = new QueuesHolder(available, process, "a");

        available.add("late");
        process.add("late");

        assertThat(h.availableQueues).containsExactly("a");
        assertThat(h.queuesToProcess).containsExactly("a");
    }
}
