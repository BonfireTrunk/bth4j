package today.bonfire.oss.bth4j.redis;

import java.util.HashMap;
import java.util.Map;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.UnifiedJedis;

/**
 * What the server counts while a call runs: calls per command, scripts included, and its reads on client
 * connections, which are one per round trip of a client that sends a pipeline at once. The counts do not depend on
 * the clock. The server is reset first, so use it with one client at a time on the server.
 */
final class ServerCounters {

    record Counts(Map<String, Long> calls, long roundTrips) {
        long calls(String command) {
            return calls.getOrDefault(command, 0L);
        }
    }

    private ServerCounters() {}

    static Counts measure(UnifiedJedis jedis, Runnable call) {
        jedis.sendCommand(Protocol.Command.CONFIG, "RESETSTAT");
        var before = reads(jedis);
        call.run();
        var after = reads(jedis);
        var calls = new HashMap<String, Long>();
        for (var line : jedis.info("commandstats").split("\r?\n")) {
            if (line.startsWith("cmdstat_")) {
                calls.put(
                        line.substring("cmdstat_".length(), line.indexOf(':')),
                        Long.parseLong(line.replaceAll(".*:calls=(\\d+),.*", "$1")));
            }
        }
        calls.remove("info");
        return new Counts(calls, after - before - 1);
    }

    private static long reads(UnifiedJedis jedis) {
        for (var line : jedis.info("stats").split("\r?\n")) {
            if (line.startsWith("total_reads_processed:")) {
                return Long.parseLong(line.substring(line.indexOf(':') + 1).trim());
            }
        }
        throw new IllegalStateException("no total_reads_processed in INFO stats");
    }
}
