package today.bonfire.oss.bth4j.redis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/**
 * Runs Lua scripts by SHA-1 and loads a script on demand when the server does not know it,
 * for example after a restart or a {@code SCRIPT FLUSH}. The load is routed by the script's first key, so
 * on Redis Cluster it reaches the node that will run the script.
 */
final class ScriptRunner {

    private final UnifiedJedis jedis;
    private final Map<String, String> shaByScript = new ConcurrentHashMap<>();

    ScriptRunner(UnifiedJedis jedis) {
        this.jedis = jedis;
    }

    Object run(String script, List<String> keys, List<String> args) {
        var sha = shaByScript.computeIfAbsent(script, ScriptRunner::sha1);
        try {
            return jedis.evalsha(sha, keys, args);
        } catch (JedisNoScriptException e) {
            var loaded = jedis.scriptLoad(script, keys.getFirst());
            return jedis.evalsha(loaded, keys, args);
        }
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
