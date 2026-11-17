package today.bonfire.oss.bth4j.admin.http;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;

/** Serves files below one directory; unknown paths without an extension fall back to index.html. */
final class StaticFiles {

    /** A file read once for one request: the tag is the hash of exactly the bytes that are sent. */
    record Resource(byte[] body, String contentType, String etag) {
        boolean html() {
            return contentType.startsWith("text/html");
        }

        /** True when an If-None-Match header names this resource: a list, {@code *}, and a weak prefix all count. */
        boolean matches(String ifNoneMatch) {
            if (ifNoneMatch == null) {
                return false;
            }
            for (var candidate : ifNoneMatch.split(",")) {
                var tag = candidate.strip();
                if (tag.startsWith("W/")) {
                    tag = tag.substring(2);
                }
                if (tag.equals("*") || tag.equals(etag)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final Map<String, String> TYPES = Map.ofEntries(
            Map.entry("html", "text/html; charset=utf-8"),
            Map.entry("css", "text/css; charset=utf-8"),
            Map.entry("js", "text/javascript; charset=utf-8"),
            Map.entry("json", "application/json; charset=utf-8"),
            Map.entry("svg", "image/svg+xml"),
            Map.entry("png", "image/png"),
            Map.entry("ico", "image/x-icon"),
            Map.entry("woff2", "font/woff2"),
            Map.entry("map", "application/json; charset=utf-8"));

    private final Path root;

    StaticFiles(Path dir) {
        try {
            this.root = dir.toRealPath();
        } catch (IOException e) {
            throw new UncheckedIOException("Static directory is not readable: " + dir, e);
        }
    }

    /** @return the resource for a decoded request path, or null when there is none */
    Resource find(String requestPath) throws IOException {
        if (requestPath.indexOf('\0') >= 0) {
            return null;
        }
        var relative = requestPath.startsWith("/") ? requestPath.substring(1) : requestPath;
        var file = relative.isEmpty() ? null : resolve(relative);
        if (file == null && (relative.isEmpty() || !hasExtension(relative))) {
            file = resolve("index.html");
        }
        if (file == null) {
            return null;
        }
        var body = Files.readAllBytes(file);
        return new Resource(body, contentType(file), etag(body));
    }

    private static String etag(byte[] body) {
        try {
            var digest = MessageDigest.getInstance("SHA-256").digest(body);
            return "\"" + HexFormat.of().formatHex(digest, 0, 16) + "\"";
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java platform", e);
        }
    }

    /** Normalizing and re-checking after toRealPath keeps both ".." and symlinks from leaving the directory. */
    private Path resolve(String relative) throws IOException {
        Path candidate;
        try {
            candidate = root.resolve(relative).normalize();
        } catch (InvalidPathException e) {
            return null;
        }
        if (!candidate.startsWith(root) || !Files.isRegularFile(candidate)) {
            return null;
        }
        var real = candidate.toRealPath();
        return real.startsWith(root) ? real : null;
    }

    private static boolean hasExtension(String relative) {
        var name = relative.substring(relative.lastIndexOf('/') + 1);
        return name.indexOf('.') > 0;
    }

    private static String contentType(Path file) {
        var name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        var dot = name.lastIndexOf('.');
        return dot < 0
                ? "application/octet-stream"
                : TYPES.getOrDefault(name.substring(dot + 1), "application/octet-stream");
    }
}
