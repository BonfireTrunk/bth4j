package today.bonfire.oss.bth4j.store;

/** A leased task whose lease expired, as returned by {@link TaskStore#reclaim}. */
public record ExpiredLease(String id, String token, int attempts, int maxAttempts, String event) {}
