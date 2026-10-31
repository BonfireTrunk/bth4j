package today.bonfire.oss.bth4j.common;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal strict JSON encoder and parser without dependencies. Parsed values are {@code Map<String,Object>}
 * (insertion ordered), {@code List<Object>}, {@code String}, {@code Long} or {@code Double}, {@code Boolean}
 * and null.
 */
public final class Json {

    private static final int MAX_DEPTH = 256;

    private Json() {}

    /**
     * Parses one JSON value.
     *
     * @throws IllegalArgumentException when the text is not exactly one valid JSON value
     */
    public static Object parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("JSON text is null");
        }
        var p = new Parser(text);
        p.skipWhitespace();
        var value = p.readValue(0);
        p.skipWhitespace();
        if (p.pos != text.length()) {
            throw p.error("Unexpected trailing content");
        }
        return value;
    }

    /**
     * Encodes a value as JSON text.
     *
     * @throws IllegalArgumentException for unsupported types, non-String map keys and NaN or infinite doubles
     */
    public static String write(Object value) {
        var sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    /** Appends the JSON encoding of a value; see {@link #write(Object)}. */
    public static void write(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            writeString(sb, s);
        } else if (value instanceof Boolean b) {
            sb.append(b.booleanValue());
        } else if (value instanceof Number n) {
            if ((value instanceof Double || value instanceof Float)
                    && (Double.isNaN(n.doubleValue()) || Double.isInfinite(n.doubleValue()))) {
                throw new IllegalArgumentException("JSON cannot represent " + n);
            }
            sb.append(n);
        } else if (value instanceof Instant i) {
            writeString(sb, i.toString());
        } else if (value instanceof Map<?, ?> m) {
            sb.append('{');
            var first = true;
            for (var e : m.entrySet()) {
                if (!(e.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("JSON object keys must be strings: " + e.getKey());
                }
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeString(sb, key);
                sb.append(':');
                write(sb, e.getValue());
            }
            sb.append('}');
        } else if (value instanceof Iterable<?> it) {
            sb.append('[');
            var first = true;
            for (var o : it) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                write(sb, o);
            }
            sb.append(']');
        } else if (value instanceof Object[] arr) {
            sb.append('[');
            for (var i = 0; i < arr.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                write(sb, arr[i]);
            }
            sb.append(']');
        } else {
            throw new IllegalArgumentException(
                    "Unsupported JSON type: " + value.getClass().getName());
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (var i = 0; i < s.length(); i++) {
            var c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\t' -> sb.append("\\t");
                case '\r' -> sb.append("\\r");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append("\\u00");
                        sb.append(Character.forDigit(c >> 4, 16));
                        sb.append(Character.forDigit(c & 0xf, 16));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        IllegalArgumentException error(String message) {
            return new IllegalArgumentException("Invalid JSON at offset " + pos + ": " + message);
        }

        void skipWhitespace() {
            while (pos < s.length()) {
                var c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        Object readValue(int depth) {
            if (depth > MAX_DEPTH) {
                throw error("Nesting deeper than " + MAX_DEPTH);
            }
            if (pos >= s.length()) {
                throw error("Unexpected end of input");
            }
            var c = s.charAt(pos);
            switch (c) {
                case '{':
                    return readObject(depth);
                case '[':
                    return readArray(depth);
                case '"':
                    return readString();
                case 't':
                    expectLiteral("true");
                    return Boolean.TRUE;
                case 'f':
                    expectLiteral("false");
                    return Boolean.FALSE;
                case 'n':
                    expectLiteral("null");
                    return null;
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        return readNumber();
                    }
                    throw error("Unexpected character '" + c + "'");
            }
        }

        void expectLiteral(String literal) {
            if (!s.startsWith(literal, pos)) {
                throw error("Expected " + literal);
            }
            pos += literal.length();
        }

        Map<String, Object> readObject(int depth) {
            var map = new LinkedHashMap<String, Object>();
            pos++;
            skipWhitespace();
            if (peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWhitespace();
                if (peek() != '"') {
                    throw error("Expected a string key");
                }
                var key = readString();
                skipWhitespace();
                if (peek() != ':') {
                    throw error("Expected ':'");
                }
                pos++;
                skipWhitespace();
                map.put(key, readValue(depth + 1));
                skipWhitespace();
                var c = peek();
                pos++;
                if (c == '}') {
                    return map;
                }
                if (c != ',') {
                    pos--;
                    throw error("Expected ',' or '}'");
                }
            }
        }

        List<Object> readArray(int depth) {
            var list = new ArrayList<Object>();
            pos++;
            skipWhitespace();
            if (peek() == ']') {
                pos++;
                return list;
            }
            while (true) {
                skipWhitespace();
                list.add(readValue(depth + 1));
                skipWhitespace();
                var c = peek();
                pos++;
                if (c == ']') {
                    return list;
                }
                if (c != ',') {
                    pos--;
                    throw error("Expected ',' or ']'");
                }
            }
        }

        int peek() {
            return pos < s.length() ? s.charAt(pos) : -1;
        }

        String readString() {
            pos++;
            var sb = new StringBuilder();
            while (true) {
                if (pos >= s.length()) {
                    throw error("Unterminated string");
                }
                var c = s.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c < 0x20) {
                    pos--;
                    throw error("Unescaped control character");
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (pos >= s.length()) {
                    throw error("Unterminated string");
                }
                var e = s.charAt(pos++);
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> sb.append(readUnicode());
                    default -> {
                        pos--;
                        throw error("Invalid escape '\\" + e + "'");
                    }
                }
            }
        }

        char readUnicode() {
            if (pos + 4 > s.length()) {
                throw error("Incomplete \\u escape");
            }
            var v = 0;
            for (var i = 0; i < 4; i++) {
                var d = Character.digit(s.charAt(pos + i), 16);
                if (d < 0) {
                    throw error("Invalid \\u escape");
                }
                v = v * 16 + d;
            }
            pos += 4;
            return (char) v;
        }

        Object readNumber() {
            var start = pos;
            if (peek() == '-') {
                pos++;
            }
            if (peek() == '0') {
                pos++;
            } else if (isDigit(peek())) {
                digits();
            } else {
                throw error("Invalid number");
            }
            var integral = true;
            if (peek() == '.') {
                integral = false;
                pos++;
                if (!isDigit(peek())) {
                    throw error("Invalid number");
                }
                digits();
            }
            if (peek() == 'e' || peek() == 'E') {
                integral = false;
                pos++;
                if (peek() == '+' || peek() == '-') {
                    pos++;
                }
                if (!isDigit(peek())) {
                    throw error("Invalid number");
                }
                digits();
            }
            var literal = s.substring(start, pos);
            if (integral) {
                try {
                    return Long.parseLong(literal);
                } catch (NumberFormatException ignored) {
                    // beyond the long range, falls through to Double
                }
            }
            var d = Double.parseDouble(literal);
            if (Double.isInfinite(d)) {
                pos = start;
                throw error("Number out of range");
            }
            return d;
        }

        void digits() {
            while (isDigit(peek())) {
                pos++;
            }
        }

        static boolean isDigit(int c) {
            return c >= '0' && c <= '9';
        }
    }
}
