package today.bonfire.oss.bth4j.redis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/**
 * Runs Lua scripts by SHA-1 and loads a script on demand when the server does not know it,
 * for example after a restart or a {@code SCRIPT FLUSH}. Scripts declare one key, the routing key, which
 * carries the namespace hash tag; they derive every other key from the prefix that is prepended to the
 * arguments. On Redis Cluster the load is routed by the same key, so it reaches the node that runs the script.
 */
final class ScriptRunner {

    private final UnifiedJedis jedis;
    private final List<String> routing;
    private final String prefix;
    private final Map<String, String> shaByScript = new ConcurrentHashMap<>();

    ScriptRunner(UnifiedJedis jedis, String routingKey, String prefix) {
        this.jedis = jedis;
        this.routing = List.of(routingKey);
        this.prefix = prefix;
    }

    Object run(String script, List<String> args) {
        var sha = shaByScript.computeIfAbsent(script, ScriptRunner::sha1);
        var full = withPrefix(args);
        try {
            return jedis.evalsha(sha, routing, full);
        } catch (JedisNoScriptException e) {
            return jedis.evalsha(jedis.scriptLoad(script, routing.getFirst()), routing, full);
        }
    }

    /**
     * One pipelined round trip for all calls. When the server rejected some as unknown, the script is loaded
     * once and only those calls are pipelined again; results stay in call order.
     */
    List<Object> runAll(String script, List<List<String>> calls) {
        var sha = shaByScript.computeIfAbsent(script, ScriptRunner::sha1);
        var results = new Object[calls.size()];
        var missing =
                pipeline(sha, calls, IntStream.range(0, calls.size()).boxed().toList(), results);
        if (!missing.isEmpty()) {
            jedis.scriptLoad(script, routing.getFirst());
            for (var i : pipeline(sha, calls, missing, results)) {
                results[i] = run(script, calls.get(i));
            }
        }
        return Arrays.asList(results);
    }

    /** Sends the calls at the given indexes in one pipeline, stores their replies and returns the indexes the server did not know the script for. */
    private List<Integer> pipeline(String sha, List<List<String>> calls, List<Integer> indexes, Object[] results) {
        var responses = new ArrayList<Response<Object>>(indexes.size());
        try (var pipeline = jedis.pipelined()) {
            for (var i : indexes) {
                responses.add(pipeline.evalsha(sha, routing, withPrefix(calls.get(i))));
            }
            pipeline.sync();
        }
        var missing = new ArrayList<Integer>();
        for (var k = 0; k < indexes.size(); k++) {
            try {
                results[indexes.get(k)] = responses.get(k).get();
            } catch (JedisNoScriptException e) {
                missing.add(indexes.get(k));
            }
        }
        return missing;
    }

    private List<String> withPrefix(List<String> args) {
        var full = new ArrayList<String>(args.size() + 1);
        full.add(prefix);
        full.addAll(args);
        return full;
    }

    private static String sha1(String script) {
        try {
            var digest = MessageDigest.getInstance("SHA-1").digest(script.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
