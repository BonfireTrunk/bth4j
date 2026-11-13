package today.bonfire.oss.bth4j.sqlite;

import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import today.bonfire.oss.bth4j.fault.Faulty;
import today.bonfire.oss.bth4j.spi.FaultContract;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * {@link FaultContract} against {@link SqliteTaskStore} on a temporary database file, with a clock the test moves.
 * A fault point is a round trip to SQLite through the JDBC driver: each statement and each commit. A cut aborts
 * the connection, which rolls back the open transaction.
 */
class SqliteFaultContractTest extends FaultContract {

    private static SqliteTestSupport database;
    private static SqliteFaults faults;

    private final MutableClock clock = new MutableClock();

    @BeforeAll
    static void openDatabase() {
        database = SqliteTestSupport.open();
        faults = new SqliteFaults(database);
    }

    @AfterAll
    static void closeDatabase() {
        faults.close();
        database.close();
    }

    @Override
    protected Faulty newFaulty(String namespace) {
        return faults.faulty(namespace, clock);
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
