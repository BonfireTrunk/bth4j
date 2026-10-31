package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import today.bonfire.oss.bth4j.common.Json;

/** JSON text forms of the map and list fields of {@link TaskRecord}, shared by the store implementations. */
public final class RecordJson {

    private RecordJson() {}

    /** @return a JSON object of strings, or null for a null or empty map */
    public static String encodeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        return Json.write(headers);
    }

    /**
     * @return the headers in text order, or null for null or blank text
     * @throws IllegalArgumentException when the text is not a JSON object of strings
     */
    public static Map<String, String> decodeHeaders(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        var map = object(Json.parse(text), "headers");
        var out = new LinkedHashMap<String, String>();
        for (var e : map.entrySet()) {
            out.put(e.getKey(), string(e.getValue(), "headers." + e.getKey(), true));
        }
        return out;
    }

    /** @return a JSON object of step name to {at, result}, or null for a null or empty map */
    public static String encodeSteps(Map<String, StepResult> steps) {
        if (steps == null || steps.isEmpty()) {
            return null;
        }
        var out = new LinkedHashMap<String, Object>();
        for (var e : steps.entrySet()) {
            var step = e.getValue();
            var m = new LinkedHashMap<String, Object>();
            m.put("at", step.at());
            if (step.result() != null) {
                m.put("result", step.result());
            }
            out.put(e.getKey(), m);
        }
        return Json.write(out);
    }

    /**
     * @return the steps in text order, or null for null or blank text
     * @throws IllegalArgumentException when the text is not a valid steps object
     */
    public static Map<String, StepResult> decodeSteps(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        var map = object(Json.parse(text), "steps");
        var out = new LinkedHashMap<String, StepResult>();
        for (var e : map.entrySet()) {
            var where = "steps." + e.getKey();
            var m = object(e.getValue(), where);
            var at = instant(m.get("at"), where + ".at");
            out.put(e.getKey(), new StepResult(at, string(m.get("result"), where + ".result", true)));
        }
        return out;
    }

    /** @return a JSON array of failures with null fields omitted, or null for a null or empty list */
    public static String encodeFailures(List<FailureInfo> failures) {
        if (failures == null || failures.isEmpty()) {
            return null;
        }
        var list = new ArrayList<Object>(failures.size());
        for (var f : failures) {
            var m = new LinkedHashMap<String, Object>();
            m.put("attempt", f.attempt());
            if (f.at() != null) {
                m.put("at", f.at());
            }
            if (f.type() != null) {
                m.put("type", f.type());
            }
            if (f.message() != null) {
                m.put("message", f.message());
            }
            if (f.stack() != null) {
                m.put("stack", f.stack());
            }
            list.add(m);
        }
        return Json.write(list);
    }

    /**
     * @return the failures in text order, or null for null or blank text
     * @throws IllegalArgumentException when the text is not a valid failures array
     */
    public static List<FailureInfo> decodeFailures(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        if (!(Json.parse(text) instanceof List<?> list)) {
            throw new IllegalArgumentException("failures must be a JSON array");
        }
        var out = new ArrayList<FailureInfo>(list.size());
        for (var i = 0; i < list.size(); i++) {
            var where = "failures[" + i + "]";
            var m = object(list.get(i), where);
            if (!(m.get("attempt") instanceof Long attempt) || attempt < 0 || attempt > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(where + ".attempt must be an integer");
            }
            var at = m.get("at") == null ? null : instant(m.get("at"), where + ".at");
            out.add(new FailureInfo(
                    attempt.intValue(),
                    at,
                    string(m.get("type"), where + ".type", true),
                    string(m.get("message"), where + ".message", true),
                    string(m.get("stack"), where + ".stack", true)));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value, String where) {
        if (value instanceof Map<?, ?>) {
            return (Map<String, Object>) value;
        }
        throw new IllegalArgumentException(where + " must be a JSON object");
    }

    private static String string(Object value, String where, boolean nullable) {
        if (value == null && nullable) {
            return null;
        }
        if (value instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException(where + " must be a string");
    }

    private static Instant instant(Object value, String where) {
        var s = string(value, where, false);
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(where + " is not an ISO-8601 instant: " + s, e);
        }
    }
}
