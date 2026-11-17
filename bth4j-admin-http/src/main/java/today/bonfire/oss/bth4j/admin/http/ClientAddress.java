package today.bonfire.oss.bth4j.admin.http;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/**
 * Finds the address a request came from, for the login throttle. The address is the peer of the connection,
 * unless that peer is a trusted proxy: then it is the last address in {@code X-Forwarded-For} that is not a
 * trusted proxy itself. Reading from the right matters: a proxy appends the address it saw, so everything to
 * the left of that was sent by the client and can be made up.
 */
final class ClientAddress {

    private record Range(byte[] network, int prefixBits) {
        boolean contains(InetAddress address) {
            var bytes = address.getAddress();
            if (bytes.length != network.length) return false;
            int whole = prefixBits / 8;
            for (int i = 0; i < whole; i++) {
                if (bytes[i] != network[i]) return false;
            }
            int rest = prefixBits % 8;
            if (rest == 0) return true;
            int mask = 0xFF << (8 - rest) & 0xFF;
            return (bytes[whole] & mask) == (network[whole] & mask);
        }
    }

    private static final int LONGEST_LITERAL = 45;

    private final List<Range> trusted;

    private ClientAddress(List<Range> trusted) {
        this.trusted = trusted;
    }

    /**
     * @param proxies addresses such as {@code 10.0.0.5} or {@code ::1} and ranges such as {@code 10.0.0.0/8}
     * @throws TaskConfigurationException when an entry is neither; the message gives its position, not its text
     */
    static ClientAddress trusting(Collection<String> proxies) {
        var ranges = new ArrayList<Range>();
        int position = 0;
        for (var proxy : proxies) {
            position++;
            var range = proxy == null ? null : range(proxy.strip());
            if (range == null) {
                throw new TaskConfigurationException(
                        "trusted proxy " + position + " is not an IP address or a range such as 10.0.0.0/8");
            }
            ranges.add(range);
        }
        return new ClientAddress(List.copyOf(ranges));
    }

    private static Range range(String text) {
        int slash = text.indexOf('/');
        var address = literal(slash < 0 ? text : text.substring(0, slash));
        if (address == null) return null;
        int bits = address.getAddress().length * 8;
        if (slash < 0) return new Range(address.getAddress(), bits);
        var length = text.substring(slash + 1);
        if (length.isEmpty() || length.length() > 3 || !length.chars().allMatch(c -> c >= '0' && c <= '9')) {
            return null;
        }
        int prefix = Integer.parseInt(length);
        return prefix > bits ? null : new Range(address.getAddress(), prefix);
    }

    boolean trusts(InetAddress address) {
        for (var range : trusted) {
            if (range.contains(address)) return true;
        }
        return false;
    }

    /**
     * @param peer         the other end of the connection
     * @param forwardedFor the values of the {@code X-Forwarded-For} header lines in the order received; may be null
     * @return the peer when it is not a trusted proxy; otherwise the nearest forwarded address that is not one. An
     *         entry that is not an IP address ends the walk, so the answer is then the proxy that reported it.
     */
    InetAddress resolve(InetAddress peer, List<String> forwardedFor) {
        if (forwardedFor == null || !trusts(peer)) return peer;
        var client = peer;
        for (int line = forwardedFor.size() - 1; line >= 0; line--) {
            var entries = forwardedFor.get(line).split(",", -1);
            for (int i = entries.length - 1; i >= 0; i--) {
                var address = literal(withoutPort(entries[i].strip()));
                if (address == null) return client;
                client = address;
                if (!trusts(address)) return client;
            }
        }
        return client;
    }

    /** @return the key the throttle counts by: the address, or its /64 network for IPv6, where one client owns many */
    static String throttleKey(InetAddress address) {
        if (address instanceof Inet6Address) {
            return HexFormat.of().formatHex(address.getAddress(), 0, 8) + "/64";
        }
        return address.getHostAddress();
    }

    /** Some proxies add the port: {@code 203.0.113.7:4711} or {@code [2001:db8::1]:4711}. */
    private static String withoutPort(String entry) {
        if (entry.startsWith("[")) {
            int close = entry.indexOf(']');
            return close < 0 ? entry : entry.substring(1, close);
        }
        int colon = entry.indexOf(':');
        return colon > 0 && colon == entry.lastIndexOf(':') ? entry.substring(0, colon) : entry;
    }

    /**
     * Reads an IP address written as text and never asks a name service, which {@link InetAddress#getByName}
     * does for anything it does not take for an address; the text can come from a request header.
     *
     * @return null when the text is not an IPv4 or IPv6 address
     */
    static InetAddress literal(String text) {
        if (text.isEmpty() || text.length() > LONGEST_LITERAL) return null;
        try {
            if (text.indexOf(':') < 0) {
                var bytes = ipv4(text);
                return bytes == null ? null : InetAddress.getByAddress(bytes);
            }
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c != ':' && c != '.' && Character.digit(c, 16) < 0) return null;
            }
            // in brackets the text is only ever parsed as an IPv6 address
            return InetAddress.getByName('[' + text + ']');
        } catch (UnknownHostException e) {
            return null;
        }
    }

    private static byte[] ipv4(String text) {
        var parts = text.split("\\.", -1);
        if (parts.length != 4) return null;
        var bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            var part = parts[i];
            if (part.isEmpty() || part.length() > 3 || !part.chars().allMatch(c -> c >= '0' && c <= '9')) {
                return null;
            }
            int value = Integer.parseInt(part);
            if (value > 255) return null;
            bytes[i] = (byte) value;
        }
        return bytes;
    }
}
