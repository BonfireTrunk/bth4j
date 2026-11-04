package today.bonfire.oss.bth4j;

import java.lang.reflect.InvocationTargetException;

/**
 * Runs one round of a service's private loop body on the calling thread, so that a test drives the service
 * with its manual clock instead of waiting for its scheduler thread.
 */
final class PrivateMethods {
    private PrivateMethods() {}

    static void invoke(Object target, String name) throws Exception {
        var method = target.getClass().getDeclaredMethod(name);
        method.setAccessible(true);
        try {
            method.invoke(target);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            if (e.getCause() instanceof Error cause) throw cause;
            throw e;
        }
    }
}
