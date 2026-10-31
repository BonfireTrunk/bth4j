package today.bonfire.oss.bth4j.testing;

import today.bonfire.oss.bth4j.ThroughputContract;
import today.bonfire.oss.bth4j.spi.FullStore;

class InMemoryThroughputTest extends ThroughputContract {

    @Override
    protected FullStore newStore(String namespace) {
        return new InMemoryStore();
    }
}
