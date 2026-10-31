package today.bonfire.oss.bth4j.redis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    /** One pipelined round trip for all calls; calls the server rejected as unknown are loaded and retried. */
    List<Object> runAll(String script, List<List<String>> calls) {
        var sha = shaByScript.computeIfAbsent(script, ScriptRunner::sha1);
        var results = new ArrayList<Object>(calls.size());
        var responses = new ArrayList<Response<Object>>(calls.size());
        try (var pipeline = jedis.pipelined()) {
            for (var args : calls) {
                responses.add(pipeline.evalsha(sha, routing, withPrefix(args)));
            }
            pipeline.sync();
        }
        for (var i = 0; i < calls.size(); i++) {
            try {
                results.add(responses.get(i).get());
            } catch (JedisNoScriptException e) {
                results.add(run(script, calls.get(i)));
            }
        }
        return results;
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
