package today.bonfire.oss.bth4j.postgres;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import today.bonfire.oss.bth4j.RunnerFaultContract;
import today.bonfire.oss.bth4j.fault.Faulty;

/** {@link RunnerFaultContract} against {@link PostgresTaskStore} on a real PostgreSQL. Needs Docker. */
class PostgresRunnerFaultIT extends RunnerFaultContract {

    private static PostgresFaults faults;

    @BeforeAll
    static void openPool() {
        faults = new PostgresFaults();
    }

    @AfterAll
    static void closePool() {
        faults.close();
    }

    @Override
    protected Faulty newFaulty(String namespace) {
        return faults.faulty(namespace);
    }
}
