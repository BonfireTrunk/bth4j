package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

/** What a recurring definition does when a fire time was missed by more than the misfire threshold. */
public sealed interface Misfire {
    /** no occurrence for the missed fire; continue from now */
    record Skip() implements Misfire {}

    /** one occurrence now, however many fires were missed; continue from now (the default) */
    record FireOnceNow() implements Misfire {}

    /** one occurrence per missed fire, up to {@code max}; continue from now */
    record CatchUp(int max) implements Misfire {
        public CatchUp {
            if (max < 1) throw new TaskConfigurationError("CatchUp max must be at least 1");
        }
    }

    Misfire SKIP = new Skip();
    Misfire FIRE_ONCE_NOW = new FireOnceNow();

    static Misfire catchUp(int max) {
        return new CatchUp(max);
    }
}
