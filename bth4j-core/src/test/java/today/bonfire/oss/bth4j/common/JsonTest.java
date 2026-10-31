package today.bonfire.oss.bth4j.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {

    @Test
    void parsesScalars() {
        assertThat(Json.parse("null")).isNull();
        assertThat(Json.parse(" true ")).isEqualTo(true);
        assertThat(Json.parse("false")).isEqualTo(false);
        assertThat(Json.parse("\"abc\"")).isEqualTo("abc");
    }

    @Test
    void parsesNumbers() {
        assertThat(Json.parse("0")).isEqualTo(0L);
        assertThat(Json.parse("-17")).isEqualTo(-17L);
        assertThat(Json.parse("9223372036854775807")).isEqualTo(Long.MAX_VALUE);
        assertThat(Json.parse("-9223372036854775808")).isEqualTo(Long.MIN_VALUE);
        assertThat(Json.parse("9223372036854775808")).isEqualTo(9.223372036854775808E18);
        assertThat(Json.parse("1.5")).isEqualTo(1.5);
        assertThat(Json.parse("-0.25")).isEqualTo(-0.25);
        assertThat(Json.parse("1e3")).isEqualTo(1000.0);
        assertThat(Json.parse("2E-2")).isEqualTo(0.02);
        assertThat(Json.parse("1.5e+2")).isEqualTo(150.0);
    }

    @Test
    void rejectsInvalidNumbers() {
        for (var bad : List.of("01", "-", "1.", ".5", "1e", "1e+", "+1", "--1", "0x10", "1e999", "NaN")) {
            assertThatThrownBy(() -> Json.parse(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void parsesNestedStructuresKeepingOrder() {
        var parsed = Json.parse("{\"z\":1,\"a\":[1,{\"k\":null},[true]],\"m\":{}}");
        assertThat(parsed).isInstanceOf(LinkedHashMap.class);
        @SuppressWarnings("unchecked")
        var map = (Map<String, Object>) parsed;
        assertThat(map.keySet()).containsExactly("z", "a", "m");
        assertThat(map.get("z")).isEqualTo(1L);
        var list = (List<?>) map.get("a");
        assertThat(list).hasSize(3);
        assertThat(((Map<?, ?>) list.get(1)).containsKey("k")).isTrue();
        assertThat(((Map<?, ?>) list.get(1)).get("k")).isNull();
        assertThat(list.get(2)).isEqualTo(List.of(true));
        assertThat((Map<?, ?>) map.get("m")).isEmpty();
    }

    @Test
    void parsesEscapes() {
        assertThat(Json.parse("\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\t\"")).isEqualTo("a\"b\\c/d\b\f\n\r\t");
        assertThat(Json.parse("\"\\u0041\\u00e9\"")).isEqualTo("A\u00e9");
        assertThat(Json.parse("\"\\ud83d\\ude00\"")).isEqualTo("\uD83D\uDE00");
    }

    @Test
    void rejectsMalformedText() {
        var bad = List.of(
                "",
                "   ",
                "{",
                "[1,",
                "[1 2]",
                "[1,]",
                "{\"a\":1,}",
                "{\"a\" 1}",
                "{a:1}",
                "{\"a\":}",
                "\"abc",
                "\"a\\",
                "\"\\x\"",
                "\"\\u12\"",
                "\"\\u12g4\"",
                "\"a\nb\"",
                "true false",
                "{} x",
                "nul",
                "tru",
                "'a'");
        for (var text : bad) {
            assertThatThrownBy(() -> Json.parse(text)).as(text).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> Json.parse(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsExcessiveNesting() {
        var deep = "[".repeat(1000) + "]".repeat(1000);
        assertThatThrownBy(() -> Json.parse(deep))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Nesting");
    }

    @Test
    void writesScalars() {
        assertThat(Json.write(null)).isEqualTo("null");
        assertThat(Json.write(true)).isEqualTo("true");
        assertThat(Json.write(42)).isEqualTo("42");
        assertThat(Json.write(-7L)).isEqualTo("-7");
        assertThat(Json.write(1.5)).isEqualTo("1.5");
        assertThat(Json.write("x")).isEqualTo("\"x\"");
        assertThat(Json.write(Instant.parse("2026-01-02T03:04:05.123Z"))).isEqualTo("\"2026-01-02T03:04:05.123Z\"");
    }

    @Test
    void writesContainers() {
        var map = new LinkedHashMap<String, Object>();
        map.put("b", 1);
        map.put("a", List.of("x", true));
        map.put("n", null);
        assertThat(Json.write(map)).isEqualTo("{\"b\":1,\"a\":[\"x\",true],\"n\":null}");
        assertThat(Json.write(new Object[] {1, "two", null})).isEqualTo("[1,\"two\",null]");
        assertThat(Json.write(Map.of())).isEqualTo("{}");
        assertThat(Json.write(List.of())).isEqualTo("[]");
    }

    @Test
    void writeAppendsToBuilder() {
        var sb = new StringBuilder("x=");
        Json.write(sb, List.of(1, 2));
        assertThat(sb.toString()).isEqualTo("x=[1,2]");
    }

    @Test
    void escapesSpecialCharacters() {
        assertThat(Json.write("a\"b\\c\n\t\r\b\f")).isEqualTo("\"a\\\"b\\\\c\\n\\t\\r\\b\\f\"");
        assertThat(Json.write("\u0000\u001f\u0001")).isEqualTo("\"\\u0000\\u001f\\u0001\"");
    }

    @Test
    void doesNotEscapeNonAscii() {
        assertThat(Json.write("h\u00e9llo \u4e16\u754c \uD83D\uDE00"))
                .isEqualTo("\"h\u00e9llo \u4e16\u754c \uD83D\uDE00\"");
    }

    @Test
    void roundTripsTrickyStrings() {
        var texts = List.of(
                "",
                "plain",
                "quote \" backslash \\ slash / end",
                "ctrl \u0000\u0001\b\t\n\u000b\f\r\u001f",
                "unicode \u00e9\u4e16 emoji \uD83D\uDE00\uD83D\uDC4D",
                "{\"json\":[1,2,3]}",
                "\\u0041 literal");
        for (var t : texts) {
            assertThat(Json.parse(Json.write(t))).as(t).isEqualTo(t);
        }
    }

    @Test
    void roundTripsStructures() {
        var inner = new LinkedHashMap<String, Object>();
        inner.put("k", List.of(1L, 2.5, "s", false));
        var root = new LinkedHashMap<String, Object>();
        root.put("inner", inner);
        root.put("none", null);
        root.put("list", new ArrayList<>(List.of(new LinkedHashMap<>(), List.of())));
        assertThat(Json.parse(Json.write(root))).isEqualTo(root);
    }

    @Test
    void rejectsNonFiniteNumbers() {
        assertThatThrownBy(() -> Json.write(Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Json.write(Double.POSITIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Json.write(List.of(Double.NEGATIVE_INFINITY)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnsupportedValues() {
        assertThatThrownBy(() -> Json.write(new Object())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Json.write(Map.of(1, "a"))).isInstanceOf(IllegalArgumentException.class);
    }
}
