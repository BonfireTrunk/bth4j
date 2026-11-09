package today.bonfire.oss.bth4j.sqlite;

import java.time.Clock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import today.bonfire.oss.bth4j.ThroughputContract;
import today.bonfire.oss.bth4j.spi.FullStore;

/** The shared throughput contract run against {@link SqliteTaskStore} on a temporary database file. Needs no Docker. */
class SqliteThroughputTest extends ThroughputContract {

    private static SqliteTestSupport database;

    @BeforeAll
    static void openDatabase() {
        database = SqliteTestSupport.open();
    }

    @AfterAll
    static void closeDatabase() {
        database.close();
    }

    @Override
    protected FullStore newStore(String namespace) {
        return database.store(namespace, Clock.systemUTC());
    }
}
