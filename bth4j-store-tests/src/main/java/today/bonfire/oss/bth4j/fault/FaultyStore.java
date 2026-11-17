package today.bonfire.oss.bth4j.fault;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.spi.FullStore;

/**
 * A {@link FullStore} that fails whole calls: the store is not called ({@link FaultPlan.Cut#BEFORE}), or it is
 * called and its answer is replaced by a {@link TaskStoreException} ({@link FaultPlan.Cut#AFTER}). Each call is
 * one fault point. For a store that has no connection to cut, such as the in-memory store, and for faults that
 * depend on which method is called.
 */
public final class FaultyStore {
    private FaultyStore() {}

    /** Decides per call, by method name, whether and how it fails; null lets the call through. */
    public interface Decision {
        FaultPlan.Cut decide(String method);
    }

    public static FullStore around(FullStore real, FaultPlan plan) {
        return around(real, method -> plan.reach());
    }

    public static FullStore around(FullStore real, Decision decision) {
        return (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, m, args) -> {
                    if (m.getDeclaringClass() == Object.class) return m.invoke(real, args);
                    var cut = decision.decide(m.getName());
                    if (cut == FaultPlan.Cut.BEFORE) {
                        throw new TaskStoreException("injected fault before " + m.getName());
                    }
                    Object result;
                    try {
                        result = m.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (cut == FaultPlan.Cut.AFTER) {
                        throw new TaskStoreException("injected fault after " + m.getName());
                    }
                    return result;
                });
    }
}
