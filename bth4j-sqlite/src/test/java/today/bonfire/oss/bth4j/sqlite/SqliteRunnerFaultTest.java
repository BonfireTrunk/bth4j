package today.bonfire.oss.bth4j.sqlite;

import java.time.Clock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import today.bonfire.oss.bth4j.RunnerFaultContract;
import today.bonfire.oss.bth4j.fault.Faulty;

/** {@link RunnerFaultContract} against {@link SqliteTaskStore} on a temporary database file. Needs no Docker. */
class SqliteRunnerFaultTest extends RunnerFaultContract {

    private static SqliteTestSupport database;
    private static SqliteFaults faults;

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
        return faults.faulty(namespace, Clock.systemUTC());
    }
}
