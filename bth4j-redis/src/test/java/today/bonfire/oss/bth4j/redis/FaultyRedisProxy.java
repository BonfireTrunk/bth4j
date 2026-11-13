package today.bonfire.oss.bth4j.redis;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import today.bonfire.oss.bth4j.fault.FaultPlan;

/**
 * A TCP proxy between Jedis and a Redis server that cuts connections at the points a {@link FaultPlan} chooses.
 * A point is one command of the client, so a pipeline of several commands has several points and can be cut part
 * way. The proxy parses the commands the client sends and the replies the server returns, so that it knows where
 * each one ends:
 * <ul>
 *   <li>before: the command is not forwarded and both sockets are closed; the server never sees it,
 *   <li>after: the command is forwarded, the server's whole reply is read and dropped, and both sockets are closed;
 *       the server did the work and the client gets a lost connection.
 * </ul>
 * The commands a client sends to set up or close a connection or to check it (HELLO, AUTH, CLIENT, SELECT, PING,
 * QUIT, RESET) are not points.
 */
final class FaultyRedisProxy implements AutoCloseable {

    private static final Set<String> NOT_POINTS = Set.of("hello", "auth", "client", "select", "ping", "quit", "reset");

    private final String host;
    private final int port;
    private final FaultPlan plan;
    private final ServerSocket listener;
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    FaultyRedisProxy(String host, int port, FaultPlan plan) throws IOException {
        this.host = host;
        this.port = port;
        this.plan = plan;
        this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofPlatform().daemon().name("faulty-redis-accept").start(this::accept);
    }

    int port() {
        return listener.getLocalPort();
    }

    private void accept() {
        while (!closed) {
            try {
                var client = listener.accept();
                var server = new Socket(host, port);
                client.setTcpNoDelay(true);
                server.setTcpNoDelay(true);
                open.add(client);
                open.add(server);
                new Connection(client, server).start();
            } catch (IOException e) {
                if (closed) return;
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            listener.close();
        } catch (IOException ignored) {
            // closing anyway
        }
        open.forEach(FaultyRedisProxy::closeQuietly);
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // closing anyway
        }
    }

    /** What to do with the reply to a command the client sent. */
    private enum Reply {
        PASS,
        DROP
    }

    private final class Connection {
        private final Socket client;
        private final Socket server;
        private final LinkedBlockingQueue<Reply> replies = new LinkedBlockingQueue<>();

        Connection(Socket client, Socket server) {
            this.client = client;
            this.server = server;
        }

        void start() {
            Thread.ofPlatform().daemon().name("faulty-redis-commands").start(this::commands);
            Thread.ofPlatform().daemon().name("faulty-redis-replies").start(this::replies);
        }

        private void endBoth() {
            closeQuietly(client);
            closeQuietly(server);
            open.remove(client);
            open.remove(server);
        }

        private void commands() {
            try {
                var in = new BufferedInputStream(client.getInputStream(), 1 << 16);
                var out = server.getOutputStream();
                while (true) {
                    var raw = new ByteArrayOutputStream();
                    var name = readCommand(in, raw);
                    if (name == null) return;
                    var cut = NOT_POINTS.contains(name) ? null : plan.reach();
                    if (cut == FaultPlan.Cut.BEFORE) return;
                    replies.add(cut == FaultPlan.Cut.AFTER ? Reply.DROP : Reply.PASS);
                    out.write(raw.toByteArray());
                    out.flush();
                }
            } catch (IOException e) {
                // a side closed
            } finally {
                endBoth();
            }
        }

        private void replies() {
            try {
                var in = new BufferedInputStream(server.getInputStream(), 1 << 16);
                var out = client.getOutputStream();
                while (true) {
                    var raw = new ByteArrayOutputStream();
                    readReply(in, raw);
                    if (replies.take() == Reply.DROP) return;
                    out.write(raw.toByteArray());
                    out.flush();
                }
            } catch (IOException e) {
                // a side closed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                endBoth();
            }
        }
    }

    /** Reads one command, an array of bulk strings, copies its bytes to {@code raw} and returns its lower case name. */
    private static String readCommand(InputStream in, ByteArrayOutputStream raw) throws IOException {
        int first = in.read();
        if (first < 0) return null;
        raw.write(first);
        if (first != '*') throw new IOException("a command is an array, not " + (char) first);
        int count = (int) line(in, raw);
        String name = null;
        for (int i = 0; i < count; i++) {
            int type = in.read();
            if (type != '$') throw new IOException("a command part is a bulk string, not " + (char) type);
            raw.write(type);
            int length = (int) line(in, raw);
            var bytes = in.readNBytes(length + 2);
            if (bytes.length != length + 2) throw new IOException("end of stream inside a command");
            raw.write(bytes);
            if (i == 0) name = new String(bytes, 0, length, StandardCharsets.US_ASCII).toLowerCase(Locale.ROOT);
        }
        return name;
    }

    /** Reads one reply of RESP2 or RESP3 and copies its bytes to {@code raw}. */
    private static void readReply(InputStream in, ByteArrayOutputStream raw) throws IOException {
        int type = in.read();
        if (type < 0) throw new IOException("end of stream");
        raw.write(type);
        switch (type) {
            case '+', '-', ':', ',', '(', '#', '_' -> line(in, raw);
            case '$', '=', '!' -> {
                long length = line(in, raw);
                if (length >= 0) {
                    var bytes = in.readNBytes((int) length + 2);
                    if (bytes.length != length + 2) throw new IOException("end of stream inside a reply");
                    raw.write(bytes);
                }
            }
            case '*', '~', '>' -> {
                long count = line(in, raw);
                for (long i = 0; i < count; i++) readReply(in, raw);
            }
            case '%' -> {
                long count = line(in, raw);
                for (long i = 0; i < 2 * count; i++) readReply(in, raw);
            }
            case '|' -> {
                long count = line(in, raw);
                for (long i = 0; i < 2 * count; i++) readReply(in, raw);
                readReply(in, raw);
            }
            default -> throw new IOException("unknown reply type " + (char) type);
        }
    }

    /** Reads up to the end of the line, copies it to {@code raw} and returns the number in it, or 0 when there is none. */
    private static long line(InputStream in, ByteArrayOutputStream raw) throws IOException {
        var text = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b < 0) throw new IOException("end of stream inside a line");
            raw.write(b);
            if (b == '\n') break;
            if (b != '\r') text.append((char) b);
        }
        try {
            return Long.parseLong(text.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
