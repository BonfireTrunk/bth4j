package today.bonfire.oss.bth4j.service;

import today.bonfire.oss.bth4j.Misfire;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;

/** Converts between {@link Misfire} and the text and catch-up count a store keeps in {@link RecurringDefinition}. */
public final class Misfires {

    private Misfires() {}

    /** @return the stored text of the policy: {@code SKIP}, {@code FIRE_ONCE_NOW} or {@code CATCH_UP} */
    public static String code(Misfire misfire) {
        return switch (misfire) {
            case Misfire.Skip s -> RecurringDefinition.MISFIRE_SKIP;
            case Misfire.FireOnceNow f -> RecurringDefinition.MISFIRE_FIRE_ONCE_NOW;
            case Misfire.CatchUp c -> RecurringDefinition.MISFIRE_CATCH_UP;
        };
    }

    /** @return the stored catch-up count of the policy; zero unless it is a catch-up */
    public static int maxFires(Misfire misfire) {
        return misfire instanceof Misfire.CatchUp c ? c.maxFires() : 0;
    }

    /** @return the policy for a stored text; null and any unknown text mean {@link Misfire#FIRE_ONCE_NOW} */
    public static Misfire fromCode(String code, int catchUpMaxFires) {
        if (code == null) return Misfire.FIRE_ONCE_NOW;
        return switch (code) {
            case RecurringDefinition.MISFIRE_SKIP -> Misfire.SKIP;
            case RecurringDefinition.MISFIRE_CATCH_UP -> Misfire.catchUp(catchUpMaxFires);
            default -> Misfire.FIRE_ONCE_NOW;
        };
    }
}
