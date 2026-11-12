package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

class QueuesTest {

    @Test
    void theFirstQueueIsTheDefaultUntilAnotherIsNamed() {
        var queues = Queues.of("main", "mail", "reports");

        assertThat(queues.names()).containsExactly("main", "mail", "reports");
        assertThat(queues.defaultQueue()).isEqualTo("main");
        assertThat(queues.withDefault("mail").defaultQueue()).isEqualTo("mail");
        assertThat(queues.withDefault("mail").names()).containsExactly("main", "mail", "reports");
        assertThat(queues.defaultQueue()).isEqualTo("main");
    }

    @Test
    void aListGivesTheSameQueuesAsTheNamesOneByOne() {
        assertThat(Queues.of(List.of("main", "mail"))).isEqualTo(Queues.of("main", "mail"));
        assertThat(Queues.of(List.of("main", "mail"))).hasSameHashCodeAs(Queues.of("main", "mail"));
        assertThat(Queues.of("main", "mail")).isNotEqualTo(Queues.of("mail", "main"));
        assertThat(Queues.of("main", "mail"))
                .isNotEqualTo(Queues.of("main", "mail").withDefault("mail"));
    }

    @Test
    void aNameGivenTwiceCountsOnceAndKeepsItsFirstPlace() {
        assertThat(Queues.of("main", "mail", "main").names()).containsExactly("main", "mail");
    }

    @Test
    void theQueuesDoNotChangeWhenTheListTheyWereMadeFromDoes() {
        var given = new ArrayList<>(List.of("main", "mail"));
        var queues = Queues.of(given);

        given.add("late");

        assertThat(queues.names()).containsExactly("main", "mail");
        assertThatThrownBy(() -> queues.names().add("x")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aDefaultQueueMustBeOneOfTheQueues() {
        assertThatThrownBy(() -> Queues.of("main").withDefault("mail"))
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("one of the queues");
        assertThatThrownBy(() -> Queues.of("main").withDefault(null)).isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void queuesWithoutANameOrWithABadNameAreRefused() {
        assertThatThrownBy(() -> Queues.of(List.of())).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Queues.of((List<String>) null)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Queues.of("main", (String[]) null)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Queues.of("main", " ")).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Queues.of((String) null)).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> Queues.of("q".repeat(129))).isInstanceOf(TaskConfigurationException.class);
        assertThat(Queues.of("q".repeat(128)).names()).hasSize(1);
    }

    @Test
    void theBuiltInQueueIsNamedCommon() {
        assertThat(Queues.COMMON).isEqualTo("common");
    }
}
