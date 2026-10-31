package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.RetryPolicy;

class RetryPoliciesTest {

    private static final RetryPolicy DEFAULT = RetryPolicy.fixed(Duration.ofSeconds(1));
    private static final RetryPolicy SLOW = RetryPolicy.fixed(Duration.ofMinutes(10));

    @Test
    void defaultPolicyIsTheOneGivenToTheConstructor() {
        var policies = new RetryPolicies(DEFAULT, Map.of("slow", SLOW));

        assertThat(policies.defaultPolicy()).isSameAs(DEFAULT);
    }

    @Test
    void nullNameResolvesToTheDefaultPolicy() {
        var policies = new RetryPolicies(DEFAULT, Map.of("slow", SLOW));

        assertThat(policies.named(null)).isSameAs(DEFAULT);
    }

    @Test
    void knownNameResolvesToItsPolicy() {
        var policies = new RetryPolicies(DEFAULT, Map.of("slow", SLOW));

        assertThat(policies.named("slow")).isSameAs(SLOW);
    }

    @Test
    void unknownNameResolvesToNullRatherThanTheDefault() {
        var policies = new RetryPolicies(DEFAULT, Map.of("slow", SLOW));

        assertThat(policies.named("missing")).isNull();
    }

    @Test
    void ofCreatesAnInstanceWithoutNamedPolicies() {
        var policies = RetryPolicies.of(DEFAULT);

        assertThat(policies.defaultPolicy()).isSameAs(DEFAULT);
        assertThat(policies.named(null)).isSameAs(DEFAULT);
        assertThat(policies.named("slow")).isNull();
    }

    @Test
    void laterChangesToTheGivenMapAreNotSeen() {
        var source = new HashMap<String, RetryPolicy>();
        source.put("slow", SLOW);
        var policies = new RetryPolicies(DEFAULT, source);

        source.put("late", DEFAULT);
        source.remove("slow");

        assertThat(policies.named("slow")).isSameAs(SLOW);
        assertThat(policies.named("late")).isNull();
    }

    @Test
    void nullNamedMapIsRejected() {
        assertThatThrownBy(() -> new RetryPolicies(DEFAULT, null)).isInstanceOf(NullPointerException.class);
    }
}
