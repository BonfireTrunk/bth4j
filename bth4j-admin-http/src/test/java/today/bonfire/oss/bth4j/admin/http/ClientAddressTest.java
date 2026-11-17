package today.bonfire.oss.bth4j.admin.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClientAddressTest {

    private static final ClientAddress PROXIES = ClientAddress.trusting(List.of("10.0.0.5", "10.1.0.0/16", "::1"));

    private static InetAddress address(String text) {
        var address = ClientAddress.literal(text);
        assertThat(address).as(text).isNotNull();
        return address;
    }

    private static String resolved(String peer, String... forwardedFor) {
        return PROXIES.resolve(address(peer), forwardedFor.length == 0 ? null : List.of(forwardedFor))
                .getHostAddress();
    }

    @Test
    void aPeerThatIsNotATrustedProxyIsTheClientWhateverItForwards() {
        assertThat(resolved("203.0.113.7")).isEqualTo("203.0.113.7");
        assertThat(resolved("203.0.113.7", "198.51.100.1")).isEqualTo("203.0.113.7");
        assertThat(resolved("10.0.0.6", "198.51.100.1")).isEqualTo("10.0.0.6");
    }

    @Test
    void behindATrustedProxyTheClientIsTheLastForwardedAddressThatIsNotAProxy() {
        assertThat(resolved("10.0.0.5", "203.0.113.7")).isEqualTo("203.0.113.7");
        assertThat(resolved("10.0.0.5", "198.51.100.1, 203.0.113.7")).isEqualTo("203.0.113.7");
        assertThat(resolved("10.0.0.5", "203.0.113.7, 10.1.2.3")).isEqualTo("203.0.113.7");
        assertThat(resolved("10.0.0.5", "198.51.100.1", "203.0.113.7, 10.1.255.255"))
                .isEqualTo("203.0.113.7");
        assertThat(resolved("::1", "2001:db8::7")).isEqualTo("2001:db8:0:0:0:0:0:7");
    }

    @Test
    void anAddressTheClientPutInTheHeaderItselfIsNotBelieved() {
        assertThat(resolved("10.0.0.5", "10.0.0.5, 203.0.113.7")).isEqualTo("203.0.113.7");
        assertThat(resolved("10.0.0.5", "127.0.0.1, 203.0.113.7")).isEqualTo("203.0.113.7");
    }

    @Test
    void aForwardedAddressMayCarryAPortOrBrackets() {
        assertThat(resolved("10.0.0.5", "203.0.113.7:4711")).isEqualTo("203.0.113.7");
        assertThat(resolved("10.0.0.5", "[2001:db8::7]:4711")).isEqualTo("2001:db8:0:0:0:0:0:7");
        assertThat(resolved("10.0.0.5", "[2001:db8::7]")).isEqualTo("2001:db8:0:0:0:0:0:7");
    }

    @Test
    void anEntryThatIsNotAnAddressEndsTheWalkAtTheProxyThatReportedIt() {
        assertThat(resolved("10.0.0.5", "unknown")).isEqualTo("10.0.0.5");
        assertThat(resolved("10.0.0.5", "203.0.113.7, unknown, 10.1.2.3")).isEqualTo("10.1.2.3");
        assertThat(resolved("10.0.0.5", "")).isEqualTo("10.0.0.5");
        assertThat(resolved("10.0.0.5", "203.0.113.7,")).isEqualTo("10.0.0.5");
    }

    @Test
    void whenEveryForwardedAddressIsAProxyTheFirstOneIsTheClient() {
        assertThat(resolved("10.0.0.5", "10.1.0.1, 10.1.0.2")).isEqualTo("10.1.0.1");
    }

    @Test
    void aRangeTrustsExactlyItsNetwork() {
        var proxies = ClientAddress.trusting(List.of("192.168.4.0/22", "2001:db8:aa00::/40", "0.0.0.0/0"));
        var narrow = ClientAddress.trusting(List.of("192.168.4.0/22", "2001:db8:aa00::/40"));

        assertThat(narrow.trusts(address("192.168.7.255"))).isTrue();
        assertThat(narrow.trusts(address("192.168.8.0"))).isFalse();
        assertThat(narrow.trusts(address("192.168.3.255"))).isFalse();
        assertThat(narrow.trusts(address("2001:db8:aaff::1"))).isTrue();
        assertThat(narrow.trusts(address("2001:db8:ab00::1"))).isFalse();
        assertThat(proxies.trusts(address("8.8.8.8")))
                .as("a /0 range is every IPv4 address")
                .isTrue();
        assertThat(proxies.trusts(address("2001:db8:ffff::1")))
                .as("and no IPv6 address")
                .isFalse();
        assertThat(ClientAddress.trusting(List.of()).trusts(address("127.0.0.1")))
                .isFalse();
    }

    @Test
    void onlyAddressesWrittenInFullAreReadAndNoNameIsLookedUp() {
        for (String bad : new String[] {
            "",
            "localhost",
            "example.com",
            "10.0.0",
            "10.0.0.256",
            "10.0.0.1.2",
            "1.2.3.-4",
            "0x7f.0.0.1",
            "abc:def",
            "fe80::1%eth0",
            "::1 ",
            "2001:db8::g",
            "1".repeat(46)
        }) {
            assertThat(ClientAddress.literal(bad)).as("'%s'", bad).isNull();
        }
        assertThat(address("::ffff:10.1.2.3").getHostAddress()).isEqualTo("10.1.2.3");
    }

    @Test
    void theThrottleCountsAnIpv4AddressByItselfAndAnIpv6AddressByItsNetwork() {
        assertThat(ClientAddress.throttleKey(address("203.0.113.7"))).isEqualTo("203.0.113.7");
        assertThat(ClientAddress.throttleKey(address("2001:db8:1:2::1")))
                .isEqualTo(ClientAddress.throttleKey(address("2001:db8:1:2:ffff:ffff:ffff:ffff")))
                .isEqualTo("20010db800010002/64");
        assertThat(ClientAddress.throttleKey(address("2001:db8:1:3::1"))).isNotEqualTo("20010db800010002/64");
    }
}
