package today.bonfire.oss.bth4j.service;

import java.util.Map;
import today.bonfire.oss.bth4j.RetryPolicy;

/** The default retry policy plus the named ones a request may choose with {@code TaskRequest.retryPolicy}. */
public final class RetryPolicies {
    private final RetryPolicy defaultPolicy;
    private final Map<String, RetryPolicy> named;

    public RetryPolicies(RetryPolicy defaultPolicy, Map<String, RetryPolicy> named) {
        this.defaultPolicy = defaultPolicy;
        this.named = Map.copyOf(named);
    }

    public static RetryPolicies of(RetryPolicy defaultPolicy) {
        return new RetryPolicies(defaultPolicy, Map.of());
    }

    /** @return the policy with that name, or null when the name is unknown */
    public RetryPolicy named(String name) {
        return name == null ? defaultPolicy : named.get(name);
    }

    public RetryPolicy defaultPolicy() {
        return defaultPolicy;
    }
}
