package today.bonfire.oss.bth4j.postgres;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.spi.FaultContract;

/**
 * {@link FaultContract} against {@link PostgresTaskStore} on a real PostgreSQL. A fault point is a round trip to
 * the server through the JDBC driver: each statement and each commit. A cut aborts the connection, so the server
 * rolls back the open transaction. Needs Docker.
 */
class PostgresFaultContractIT extends FaultContract {

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
