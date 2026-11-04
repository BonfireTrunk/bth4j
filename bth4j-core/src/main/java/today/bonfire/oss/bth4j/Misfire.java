package today.bonfire.oss.bth4j;

import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/** What a recurring definition does when a fire time was missed by more than the misfire threshold. */
public sealed interface Misfire {
    /** no occurrence for the missed fire; continue from now */
    record Skip() implements Misfire {}

    /** one occurrence now, however many fires were missed; continue from now (the default) */
    record FireOnceNow() implements Misfire {}

    /** one occurrence per missed fire, up to {@code maxFires}; continue from now */
    record CatchUp(int maxFires) implements Misfire {
        public CatchUp {
            if (maxFires < 1) throw new TaskConfigurationException("CatchUp maxFires must be at least 1");
        }
    }

    Misfire SKIP = new Skip();
    Misfire FIRE_ONCE_NOW = new FireOnceNow();

    static Misfire catchUp(int maxFires) {
        return new CatchUp(maxFires);
    }
}
