package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

class TaskTypeTest {

    record Payload(String name) {}

    @Test
    void typedTypeHasPayloadAndDefaultsToVersionOne() {
        var type = TaskType.of("send-mail", Payload.class);

        assertThat(type.name()).isEqualTo("send-mail");
        assertThat(type.hasPayload()).isTrue();
        assertThat(type.payloadClass()).isEqualTo(Payload.class);
        assertThat(type.codec()).isNull();
        assertThat(type.payloadVersion()).isEqualTo(1);
    }

    @Test
    void voidTypeHasNoPayload() {
        var type = TaskType.of("cleanup");

        assertThat(type.hasPayload()).isFalse();
        assertThat(type.payloadClass()).isEqualTo(Void.class);
    }

    @Test
    void codecTypeKeepsItsCodecAndHasPayload() {
        PayloadCodec<String> codec = new PayloadCodec<>() {
            @Override
            public String encode(String payload) {
                return payload;
            }

            @Override
            public String decode(String text) {
                return text;
            }
        };

        var type = TaskType.of("raw", codec);

        assertThat(type.codec()).isSameAs(codec);
        assertThat(type.payloadClass()).isNull();
        assertThat(type.hasPayload()).isTrue();
    }

    @Test
    void payloadVersionReturnsACopyWithTheNewVersion() {
        var v1 = TaskType.of("send-mail", Payload.class);
        var v2 = v1.payloadVersion(2);

        assertThat(v1.payloadVersion()).isEqualTo(1);
        assertThat(v2.payloadVersion()).isEqualTo(2);
        assertThat(v2.name()).isEqualTo("send-mail");
        assertThat(v2.payloadClass()).isEqualTo(Payload.class);
    }

    @Test
    void payloadVersionBelowOneIsRejected() {
        var type = TaskType.of("send-mail", Payload.class);

        assertThatThrownBy(() -> type.payloadVersion(0)).isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void nameMustBeOneTo128Characters() {
        assertThatThrownBy(() -> TaskType.of(null)).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> TaskType.of(" ")).isInstanceOf(TaskConfigurationError.class);
        assertThatThrownBy(() -> TaskType.of("x".repeat(129))).isInstanceOf(TaskConfigurationError.class);
        assertThat(TaskType.of("x".repeat(128)).name()).hasSize(128);
    }

    @Test
    void equalityIsByNameOnly() {
        var a = TaskType.of("same", Payload.class);
        var b = TaskType.of("same", Payload.class).payloadVersion(3);

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(TaskType.of("other", Payload.class));
        assertThat(a).hasToString("same");
    }
}
