package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.JsonMapperTest;
import today.bonfire.oss.bth4j.PayloadCodec;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskType;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

class CodecsTest {

    public record Payload(String name, int count) {}

    private static final TaskType<Payload> JSON_TYPE = TaskType.of("json", Payload.class);
    private static final TaskType<Void> VOID_TYPE = TaskType.of("void");

    private final Codecs codecs = new Codecs(new JsonMapperTest());

    @Test
    void jsonPayloadRoundTrips() {
        var text = codecs.encode(JSON_TYPE, new Payload("a", 3));

        assertThat(text).contains("\"name\":\"a\"");
        assertThat(codecs.decode(JSON_TYPE, text)).isEqualTo(new Payload("a", 3));
    }

    @Test
    void voidTypeEncodesAndDecodesToNull() {
        assertThat(codecs.encode(VOID_TYPE, null)).isNull();
        assertThat(codecs.decode(VOID_TYPE, null)).isNull();
        assertThat(codecs.decode(VOID_TYPE, "ignored")).isNull();
    }

    @Test
    void typeCodecIsUsedInsteadOfTheMapper() {
        PayloadCodec<String> upper = new PayloadCodec<>() {
            @Override
            public String encode(String payload) {
                return payload.toUpperCase();
            }

            @Override
            public String decode(String text) {
                return text.toLowerCase();
            }
        };
        var type = TaskType.of("custom", upper);
        var withoutMapper = new Codecs(null);

        assertThat(withoutMapper.encode(type, "abc")).isEqualTo("ABC");
        assertThat(withoutMapper.decode(type, "ABC")).isEqualTo("abc");
    }

    @Test
    void jsonTypeWithoutMapperFailsWithADataException() {
        var withoutMapper = new Codecs(null);

        assertThatThrownBy(() -> withoutMapper.encode(JSON_TYPE, new Payload("a", 1)))
                .isInstanceOf(TaskDataException.class)
                .hasRootCauseInstanceOf(TaskDataException.class);
    }

    @Test
    void codecFailureOnEncodeIsWrapped() {
        var type = TaskType.<String>of("broken", new PayloadCodec<>() {
            @Override
            public String encode(String payload) throws Exception {
                throw new Exception("cannot encode");
            }

            @Override
            public String decode(String text) {
                return text;
            }
        });

        assertThatThrownBy(() -> codecs.encode(type, "x"))
                .isInstanceOf(TaskDataException.class)
                .hasMessageContaining("broken")
                .hasCauseInstanceOf(Exception.class);
    }

    @Test
    void payloadAtTheLimitIsAcceptedAndOneByteOverIsRejected() {
        var raw = TaskType.<String>of("raw", new PayloadCodec<>() {
            @Override
            public String encode(String payload) {
                return payload;
            }

            @Override
            public String decode(String text) {
                return text;
            }
        });

        assertThat(codecs.encode(raw, "x".repeat(TaskRequest.MAX_PAYLOAD_BYTES)))
                .hasSize(TaskRequest.MAX_PAYLOAD_BYTES);
        assertThatThrownBy(() -> codecs.encode(raw, "x".repeat(TaskRequest.MAX_PAYLOAD_BYTES + 1)))
                .isInstanceOf(TaskValidationException.class);
    }

    @Test
    void sizeLimitCountsUtf8BytesNotCharacters() {
        var raw = TaskType.<String>of("raw", new PayloadCodec<>() {
            @Override
            public String encode(String payload) {
                return payload;
            }

            @Override
            public String decode(String text) {
                return text;
            }
        });
        var twoBytesEach = "é".repeat(TaskRequest.MAX_PAYLOAD_BYTES / 2 + 1);

        assertThat(twoBytesEach.length()).isLessThan(TaskRequest.MAX_PAYLOAD_BYTES);
        assertThatThrownBy(() -> codecs.encode(raw, twoBytesEach)).isInstanceOf(TaskValidationException.class);
    }

    @Test
    void decodeWithoutStoredDataFailsForTypedTypes() {
        assertThatThrownBy(() -> codecs.decode(JSON_TYPE, null))
                .isInstanceOf(TaskDataException.class)
                .hasMessageContaining("needs a payload");
    }

    @Test
    void decodeOfGarbageFailsWithADataException() {
        assertThatThrownBy(() -> codecs.decode(JSON_TYPE, "{not json"))
                .isInstanceOf(TaskDataException.class)
                .hasMessageContaining("Unable to decode");
    }

    @Test
    void stepResultsRoundTripAsJson() {
        var text = codecs.encodeStep(new Payload("s", 9));

        assertThat(codecs.decodeStep(text, Payload.class)).isEqualTo(new Payload("s", 9));
        assertThat(codecs.decodeStep(codecs.encodeStep("text"), String.class)).isEqualTo("text");
        assertThat(codecs.decodeStep(codecs.encodeStep(42), Integer.class)).isEqualTo(42);
    }

    @Test
    void nullStepResultAndVoidStepAreNull() {
        assertThat(codecs.encodeStep(null)).isNull();
        assertThat(codecs.decodeStep(null, Payload.class)).isNull();
        assertThat(codecs.decodeStep("{}", Void.class)).isNull();
    }

    @Test
    void undecodableStepResultFailsWithADataException() {
        assertThatThrownBy(() -> codecs.decodeStep("[[", Payload.class))
                .isInstanceOf(TaskDataException.class)
                .hasMessageContaining(Payload.class.getName());
    }
}
