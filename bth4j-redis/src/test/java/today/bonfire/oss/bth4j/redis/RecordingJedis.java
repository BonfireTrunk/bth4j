package today.bonfire.oss.bth4j.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * A single-connection client for tests that records how {@link ScriptRunner} talks to Redis and can disturb it:
 * {@link #events()} lists every EVALSHA, SCRIPT LOAD and EVALSHA pipeline (with its size) in call order. It can
 * present chosen calls of the next pipeline as unknown scripts, flush the script cache again after a load, and
 * resend every EVALSHA after it was executed, as a transport that lost the reply would. Use it from one thread.
 */
final class RecordingJedis extends UnifiedJedis {

    private static final String UNKNOWN_SHA = "0".repeat(40);

    private final List<String> events = new ArrayList<>();
    private IntPredicate unknownInNextPipeline;
    private int flushesAfterLoad;
    private boolean resend;
    private boolean loseReply;

    private final Connection connection;

    private RecordingJedis(Connection connection) {
        super(connection);
        this.connection = connection;
    }

    static RecordingJedis connect(String host, int port) {
        var config = DefaultJedisClientConfig.builder()
                .resp3()
                .timeoutMillis(3000)
                .clientName("bth4j-recording-test")
                .build();
        return new RecordingJedis(new Connection(new HostAndPort(host, port), config));
    }

    List<String> events() {
        return List.copyOf(events);
    }

    long loads() {
        return events.stream().filter("load"::equals).count();
    }

    void clearEvents() {
        events.clear();
    }

    /** The calls of the next EVALSHA pipeline whose index passes are sent with a sha the server does not know. */
    void rejectInNextPipeline(IntPredicate callIndex) {
        this.unknownInNextPipeline = callIndex;
    }

    /** Flushes the script cache right after each of the next {@code loads} script loads. */
    void flushAfterLoads(int loads) {
        this.flushesAfterLoad = loads;
    }

    /** Executes every EVALSHA twice with the same arguments and returns the second reply. */
    void resendEveryEvalsha(boolean on) {
        this.resend = on;
    }

    /** The next EVALSHA or EVALSHA pipeline is executed by the server, then fails as if its reply never arrived. */
    void loseNextReply() {
        this.loseReply = true;
    }

    @Override
    public Object evalsha(String sha, List<String> keys, List<String> args) {
        events.add("evalsha");
        if (resend) {
            super.evalsha(sha, keys, args);
        }
        var reply = super.evalsha(sha, keys, args);
        if (loseReply) {
            loseReply = false;
            throw new JedisConnectionException("reply lost");
        }
        return reply;
    }

    @Override
    public String scriptLoad(String script, String sampleKey) {
        events.add("load");
        var sha = super.scriptLoad(script, sampleKey);
        if (flushesAfterLoad > 0) {
            flushesAfterLoad--;
            scriptFlush();
        }
        return sha;
    }

    @Override
    public Pipeline pipelined() {
        var unknown = unknownInNextPipeline;
        unknownInNextPipeline = null;
        return new RecordingPipeline(connection, unknown);
    }

    private final class RecordingPipeline extends Pipeline {
        private final IntPredicate unknown;
        private int evalshas;

        RecordingPipeline(Connection connection, IntPredicate unknown) {
            super(connection, false);
            this.unknown = unknown;
        }

        @Override
        public Response<Object> evalsha(String sha, List<String> keys, List<String> args) {
            var send = unknown != null && unknown.test(evalshas) ? UNKNOWN_SHA : sha;
            evalshas++;
            if (resend) {
                super.evalsha(send, keys, args);
            }
            return super.evalsha(send, keys, args);
        }

        @Override
        public void sync() {
            super.sync();
            if (loseReply && evalshas > 0) {
                loseReply = false;
                throw new JedisConnectionException("reply lost");
            }
        }

        @Override
        public void close() {
            if (evalshas > 0) {
                events.add("pipeline " + evalshas);
            }
            super.close();
        }
    }
}
