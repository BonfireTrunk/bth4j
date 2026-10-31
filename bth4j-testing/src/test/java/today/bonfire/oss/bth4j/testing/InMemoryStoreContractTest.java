package today.bonfire.oss.bth4j.testing;

import java.time.Clock;
import java.time.Duration;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.StoreContract;

class InMemoryStoreContractTest extends StoreContract {

    private static final int REGISTRY_BOUND = 50;

    private final MutableClock clock = new MutableClock();

    @Override
    protected FullStore newStore(String namespace) {
        return new InMemoryStore(clock, REGISTRY_BOUND);
    }

    @Override
    protected int registryBound() {
        return REGISTRY_BOUND;
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
