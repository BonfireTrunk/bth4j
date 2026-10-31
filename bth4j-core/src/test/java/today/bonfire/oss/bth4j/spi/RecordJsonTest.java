package today.bonfire.oss.bth4j.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RecordJsonTest {

    private static final Instant T1 = Instant.parse("2026-03-04T05:06:07.123456Z");
    private static final Instant T2 = Instant.parse("2026-03-04T05:06:09Z");

    @Test
    void headersRoundTripInOrder() {
        var headers = new LinkedHashMap<String, String>();
        headers.put("z", "1");
        headers.put("a", "quote \" and \\ and \n and 😀");
        var text = RecordJson.encodeHeaders(headers);
        var decoded = RecordJson.decodeHeaders(text);
        assertThat(decoded).isEqualTo(headers);
        assertThat(decoded.keySet()).containsExactly("z", "a");
    }

    @Test
    void headersNullAndEmptyHandling() {
        assertThat(RecordJson.encodeHeaders(null)).isNull();
        assertThat(RecordJson.encodeHeaders(Map.of())).isNull();
        assertThat(RecordJson.decodeHeaders(null)).isNull();
        assertThat(RecordJson.decodeHeaders("")).isNull();
        assertThat(RecordJson.decodeHeaders("  ")).isNull();
    }

    @Test
    void headersRejectMalformedInput() {
        for (var bad : List.of("{", "[]", "\"x\"", "{\"a\":1}", "{\"a\":null}x", "{\"a\":[\"b\"]}")) {
            assertThatThrownBy(() -> RecordJson.decodeHeaders(bad))
                    .as(bad)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void headersAcceptNullValue() {
        var decoded = RecordJson.decodeHeaders("{\"a\":null}");
        assertThat(decoded).containsEntry("a", null);
    }

    @Test
    void stepsRoundTripInOrder() {
        var steps = new LinkedHashMap<String, StepResult>();
        steps.put("second", new StepResult(T2, "ok"));
        steps.put("first", new StepResult(T1, null));
        var text = RecordJson.encodeSteps(steps);
        assertThat(text)
                .isEqualTo("{\"second\":{\"at\":\"" + T2 + "\",\"result\":\"ok\"},\"first\":{\"at\":\"" + T1 + "\"}}");
        var decoded = RecordJson.decodeSteps(text);
        assertThat(decoded).isEqualTo(steps);
        assertThat(decoded.keySet()).containsExactly("second", "first");
    }

    @Test
    void stepResultTextWithJsonCharactersRoundTripsExactly() {
        var results = List.of(
                "{\"a\":[1,2,{\"b\":null}]}",
                "[1,2,3]",
                "\"quoted\"",
                "back\\slash \\u0041",
                "line1\nline2\ttab\r",
                "ctrl \u0000\u001f",
                "emoji 😀 café",
                "  spaced  ",
                "",
                "null");
        for (var r : results) {
            var steps = Map.of("s", new StepResult(T1, r));
            var decoded = RecordJson.decodeSteps(RecordJson.encodeSteps(steps));
            assertThat(decoded.get("s").result()).as(r).isEqualTo(r);
            assertThat(decoded.get("s").at()).isEqualTo(T1);
        }
    }

    @Test
    void stepsNullAndEmptyHandling() {
        assertThat(RecordJson.encodeSteps(null)).isNull();
        assertThat(RecordJson.encodeSteps(Map.of())).isNull();
        assertThat(RecordJson.decodeSteps(null)).isNull();
        assertThat(RecordJson.decodeSteps(" ")).isNull();
    }

    @Test
    void stepsRejectMalformedInput() {
        var bad = List.of(
                "{",
                "[]",
                "{\"s\":1}",
                "{\"s\":{}}",
                "{\"s\":{\"at\":5}}",
                "{\"s\":{\"at\":\"yesterday\"}}",
                "{\"s\":{\"at\":\"2026-03-04T05:06:07Z\",\"result\":{\"x\":1}}}",
                "{\"s\":{\"at\":\"2026-03-04T05:06:07Z\",\"result\":3}}");
        for (var text : bad) {
            assertThatThrownBy(() -> RecordJson.decodeSteps(text))
                    .as(text)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void failuresRoundTripPreservingOrder() {
        var failures = List.of(
                new FailureInfo(1, T1, "java.io.IOException", "boom \"x\"", "at a.B.c(B.java:1)\n\tat d.E.f(E.java:2)"),
                new FailureInfo(2, T2, "lease-expired", null, null),
                new FailureInfo(3, T2, "t", "m😀", null));
        var decoded = RecordJson.decodeFailures(RecordJson.encodeFailures(failures));
        assertThat(decoded).isEqualTo(failures);
    }

    @Test
    void failuresOmitNullFields() {
        var text = RecordJson.encodeFailures(List.of(new FailureInfo(2, T1, "lease-expired", null, null)));
        assertThat(text).isEqualTo("[{\"attempt\":2,\"at\":\"" + T1 + "\",\"type\":\"lease-expired\"}]");
    }

    @Test
    void failuresNullAndEmptyHandling() {
        assertThat(RecordJson.encodeFailures(null)).isNull();
        assertThat(RecordJson.encodeFailures(List.of())).isNull();
        assertThat(RecordJson.decodeFailures(null)).isNull();
        assertThat(RecordJson.decodeFailures("")).isNull();
    }

    @Test
    void failuresRejectMalformedInput() {
        var bad = List.of(
                "[",
                "{}",
                "[1]",
                "[{}]",
                "[{\"attempt\":\"1\"}]",
                "[{\"attempt\":1.5}]",
                "[{\"attempt\":-1}]",
                "[{\"attempt\":99999999999}]",
                "[{\"attempt\":1,\"at\":\"nope\"}]",
                "[{\"attempt\":1,\"type\":5}]",
                "[{\"attempt\":1}] trailing");
        for (var text : bad) {
            assertThatThrownBy(() -> RecordJson.decodeFailures(text))
                    .as(text)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
