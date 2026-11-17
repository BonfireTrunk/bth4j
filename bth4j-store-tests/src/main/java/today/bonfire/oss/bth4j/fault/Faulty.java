package today.bonfire.oss.bth4j.fault;

import today.bonfire.oss.bth4j.spi.FullStore;

/**
 * The store under test in two views of one namespace: {@code store}, whose calls the {@code plan} cuts, and
 * {@code plain}, which never fails and prepares and reads the state.
 */
public record Faulty(FullStore store, FullStore plain, FaultPlan plan) {}
