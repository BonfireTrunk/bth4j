package today.bonfire.oss.bth4j;

import java.nio.charset.StandardCharsets;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * Encodes and decodes payloads and step results exactly as the runner and {@link TaskClient} store them: a
 * payload with its type's {@link PayloadCodec} when it has one, otherwise with the JSON mapper; a step result
 * with the JSON mapper. Build it with the same mapper as the runner, for example to pin the stored form of a
 * payload or step result in a test.
 */
public final class Codecs {

    private final JsonMapper mapper;

    /** @param mapper the JSON mapper the runner uses; may be null when every type has its own codec */
    public Codecs(JsonMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @return the stored text of the payload, or null for a type without payload or a null payload
     * @throws TaskDataException if the codec or mapper fails
     * @throws TaskValidationException if the text exceeds {@link TaskRequest#MAX_PAYLOAD_BYTES}
     * @throws TaskConfigurationException if the type has no codec and no mapper is configured
     */
    public <T> String encode(TaskType<T> type, T payload) {
        if (!type.hasPayload() || payload == null) return null;
        var json = type.codec() != null ? null : mapper();
        String text;
        try {
            text = json == null ? type.codec().encode(payload) : json.toJson(payload);
        } catch (Exception e) {
            throw new TaskDataException("Unable to encode the payload of task type " + type, e);
        }
        if (text != null && exceedsPayloadLimit(text)) {
            throw new TaskValidationException(
                    "payload of task type " + type + " exceeds " + TaskRequest.MAX_PAYLOAD_BYTES + " bytes");
        }
        StoredText.require("payload of task type " + type, text);
        return text;
    }

    /** A char takes one to three bytes in UTF-8, so only a text between the two bounds is encoded to count them. */
    private static boolean exceedsPayloadLimit(String text) {
        if (text.length() > TaskRequest.MAX_PAYLOAD_BYTES) return true;
        if (text.length() <= TaskRequest.MAX_PAYLOAD_BYTES / 3) return false;
        return text.getBytes(StandardCharsets.UTF_8).length > TaskRequest.MAX_PAYLOAD_BYTES;
    }

    /**
     * @return the payload of the stored text, or null for a type without payload
     * @throws TaskDataException if the text is null for a type with payload, or the codec or mapper fails
     * @throws TaskConfigurationException if the type has no codec and no mapper is configured
     */
    public <T> T decode(TaskType<T> type, String text) {
        if (!type.hasPayload()) return null;
        if (text == null) {
            throw new TaskDataException("Task type " + type + " needs a payload but the task has none");
        }
        var json = type.codec() != null ? null : mapper();
        try {
            return json == null ? type.codec().decode(text) : json.fromJson(text, type.payloadClass());
        } catch (Exception e) {
            throw new TaskDataException("Unable to decode the payload of task type " + type, e);
        }
    }

    /**
     * @return the stored text of a step result, or null for a null result
     * @throws TaskDataException if the mapper fails
     * @throws TaskConfigurationException if no mapper is configured
     */
    public String encodeStep(Object result) {
        if (result == null) return null;
        var json = mapper();
        try {
            return json.toJson(result);
        } catch (Exception e) {
            throw new TaskDataException("Unable to encode a step result", e);
        }
    }

    /**
     * @return the step result of the stored text, or null for null text or {@code Void}
     * @throws TaskDataException if the mapper fails
     * @throws TaskConfigurationException if no mapper is configured
     */
    public <R> R decodeStep(String text, Class<R> type) {
        if (text == null || type == Void.class) return null;
        var json = mapper();
        try {
            return json.fromJson(text, type);
        } catch (Exception e) {
            throw new TaskDataException("Unable to decode a step result as " + type.getName(), e);
        }
    }

    /** @throws TaskConfigurationException if no mapper is configured */
    void requireMapper() {
        mapper();
    }

    /** A missing mapper is a mistake in the set-up, not in the data: a task must not end as dead for it. */
    private JsonMapper mapper() {
        if (mapper == null) {
            throw new TaskConfigurationException(
                    "No JsonMapper configured; set one on the builder or give the task type a codec");
        }
        return mapper;
    }
}
