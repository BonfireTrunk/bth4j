package today.bonfire.oss.bth4j.testing;

import today.bonfire.oss.bth4j.RunnerFaultContract;
import today.bonfire.oss.bth4j.fault.FaultPlan;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.fault.FaultyStore;

/** {@link RunnerFaultContract} against {@link InMemoryStore}; a fault point is a whole store call. */
class InMemoryRunnerFaultTest extends RunnerFaultContract {

    @Override
    protected Faulty newFaulty(String namespace) {
        var plan = new FaultPlan();
        var plain = InMemoryStore.builder().build();
        return new Faulty(FaultyStore.around(plain, plan), plain, plan);
    }
}
