package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

class TaskProcessorRegistryTest {

    private static final TaskType<Void> A = TaskType.of("a");
    private static final TaskType<Void> B = TaskType.of("b");

    @Test
    void startsEmpty() {
        var registry = new TaskProcessorRegistry();

        assertThat(registry.isEmpty()).isTrue();
        assertThat(registry.typeNames()).isEmpty();
        assertThat(registry.get("a")).isNull();
    }

    @Test
    void registersProcessorsByTypeName() {
        TaskProcessor<Void> processor = (ctx, payload) -> {};
        var registry = new TaskProcessorRegistry().register(A, processor).register(B, (ctx, payload) -> {});

        assertThat(registry.isEmpty()).isFalse();
        assertThat(registry.typeNames()).containsExactlyInAnyOrder("a", "b");
        assertThat(registry.get("a").type()).isEqualTo(A);
        assertThat(registry.get("a").processor()).isSameAs(processor);
    }

    @Test
    void duplicateTypeNameIsRejectedAndKeepsTheFirstProcessor() {
        TaskProcessor<Void> first = (ctx, payload) -> {};
        var registry = new TaskProcessorRegistry().register(A, first);

        assertThatThrownBy(() -> registry.register(TaskType.of("a"), (ctx, payload) -> {}))
                .isInstanceOf(TaskConfigurationError.class)
                .hasMessageContaining("already registered");
        assertThat(registry.get("a").processor()).isSameAs(first);
    }

    @Test
    void nullTypeOrProcessorIsRejected() {
        var registry = new TaskProcessorRegistry();

        assertThatThrownBy(() -> registry.register(null, (ctx, p) -> {})).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> registry.register(A, null)).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void registrationIsRejectedOnceFrozen() {
        var registry = new TaskProcessorRegistry().register(A, (ctx, payload) -> {});
        registry.freeze();

        assertThatThrownBy(() -> registry.register(B, (ctx, payload) -> {}))
                .isInstanceOf(TaskConfigurationError.class)
                .hasMessageContaining("start()");
        assertThat(registry.typeNames()).containsExactly("a");
    }

    @Test
    void typeNamesIsASnapshot() {
        var registry = new TaskProcessorRegistry().register(A, (ctx, payload) -> {});
        var names = registry.typeNames();

        registry.register(B, (ctx, payload) -> {});

        assertThat(names).containsExactly("a");
    }
}
