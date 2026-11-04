package today.bonfire.oss.bth4j;

import java.util.Map;

/** The default retry policy plus the named ones a request may choose with {@code TaskRequest.retryPolicy}. */
final class RetryPolicies {
    private final RetryPolicy defaultPolicy;
    private final Map<String, RetryPolicy> named;

    RetryPolicies(RetryPolicy defaultPolicy, Map<String, RetryPolicy> named) {
        this.defaultPolicy = defaultPolicy;
        this.named = Map.copyOf(named);
    }

    static RetryPolicies of(RetryPolicy defaultPolicy) {
        return new RetryPolicies(defaultPolicy, Map.of());
    }

    /** @return the policy with that name, or null when the name is unknown */
    RetryPolicy named(String name) {
        return name == null ? defaultPolicy : named.get(name);
    }

    RetryPolicy defaultPolicy() {
        return defaultPolicy;
    }
}
