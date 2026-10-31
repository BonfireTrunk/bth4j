package today.bonfire.oss.bth4j.service;

import java.nio.charset.StandardCharsets;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskType;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/** Encodes and decodes payloads and step results with a type's codec or the configured JSON mapper. */
public final class Codecs {

    private final JsonMapper mapper;

    public Codecs(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public <T> String encode(TaskType<T> type, T payload) {
        if (!type.hasPayload() || payload == null) return null;
        String text;
        try {
            text = type.codec() != null ? type.codec().encode(payload) : mapper().toJson(payload);
        } catch (Exception e) {
            throw new TaskDataException("Unable to encode the payload of task type " + type, e);
        }
        if (text != null && text.getBytes(StandardCharsets.UTF_8).length > TaskRequest.MAX_PAYLOAD_BYTES) {
            throw new TaskValidationException(
                    "payload of task type " + type + " exceeds " + TaskRequest.MAX_PAYLOAD_BYTES + " bytes");
        }
        return text;
    }

    public <T> T decode(TaskType<T> type, String text) {
        if (!type.hasPayload()) return null;
        if (text == null) {
            throw new TaskDataException("Task type " + type + " needs a payload but the task has none");
        }
        try {
            return type.codec() != null ? type.codec().decode(text) : mapper().fromJson(text, type.payloadClass());
        } catch (Exception e) {
            throw new TaskDataException("Unable to decode the payload of task type " + type, e);
        }
    }

    public String encodeStep(Object result) {
        if (result == null) return null;
        try {
            return mapper().toJson(result);
        } catch (Exception e) {
            throw new TaskDataException("Unable to encode a step result", e);
        }
    }

    public <R> R decodeStep(String text, Class<R> type) {
        if (text == null || type == Void.class) return null;
        try {
            return mapper().fromJson(text, type);
        } catch (Exception e) {
            throw new TaskDataException("Unable to decode a step result as " + type.getName(), e);
        }
    }

    private JsonMapper mapper() {
        if (mapper == null) {
            throw new TaskDataException(
                    "No JsonMapper configured; set one on the builder or give the task type a codec");
        }
        return mapper;
    }
}
