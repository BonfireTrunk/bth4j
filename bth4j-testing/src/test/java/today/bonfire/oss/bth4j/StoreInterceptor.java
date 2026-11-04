package today.bonfire.oss.bth4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import today.bonfire.oss.bth4j.spi.FullStore;

/** Wraps a store so that a test can fail a call before or after the real store applied it, or act around it. */
final class StoreInterceptor {
    private StoreInterceptor() {}

    interface RealCall {
        Object proceed() throws Throwable;
    }

    interface Interceptor {
        Object intercept(String method, Object[] args, RealCall real) throws Throwable;
    }

    static FullStore around(FullStore real, Interceptor interceptor) {
        return (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, m, args) -> {
                    return interceptor.intercept(m.getName(), args, () -> {
                        try {
                            return m.invoke(real, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
                });
    }
}
