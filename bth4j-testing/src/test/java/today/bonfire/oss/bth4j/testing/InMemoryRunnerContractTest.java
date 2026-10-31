package today.bonfire.oss.bth4j.testing;

import today.bonfire.oss.bth4j.RunnerContract;
import today.bonfire.oss.bth4j.spi.FullStore;

class InMemoryRunnerContractTest extends RunnerContract {

    @Override
    protected FullStore newStore(String namespace) {
        return new InMemoryStore();
    }
}
