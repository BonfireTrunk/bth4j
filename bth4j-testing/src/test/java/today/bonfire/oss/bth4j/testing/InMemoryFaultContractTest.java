package today.bonfire.oss.bth4j.testing;

import java.time.Clock;
import java.time.Duration;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.fault.FaultyStore;
import today.bonfire.oss.bth4j.spi.FaultContract;

/**
 * {@link FaultContract} against {@link InMemoryStore}. The store has no connection to cut, so a fault point is a
 * whole call: it is not made, or it is made and its answer is replaced by a failure.
 */
class InMemoryFaultContractTest extends FaultContract {

    private final MutableClock clock = new MutableClock();

    @Override
    protected Faulty newFaulty(String namespace) {
        var plan = new FaultPlan();
        var plain = InMemoryStore.builder().clock(clock).build();
        return new Faulty(FaultyStore.around(plain, plan), plain, plan);
    }

    @Override
    protected Clock clock() {
        return clock;
    }

    @Override
    protected void advance(Duration duration) {
        clock.advance(duration);
    }
}
