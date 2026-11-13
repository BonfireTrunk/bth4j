package today.bonfire.oss.bth4j.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The writers of one store instance wait in line in the process instead of polling SQLite's write lock. The
 * connections here have a busy timeout of 1 ms, so a writer that has to wait on SQLite itself fails at once.
 */
@Timeout(120)
class SqliteWriteQueueTest {
    private static final int WRITERS = 8;
    private static final int WRITES = 200;
    private static final String NAMESPACE = "queued";

    private static SqliteTestSupport database;

    @BeforeAll
    static void openDatabase() {
        database = SqliteTestSupport.open();
        database.store(NAMESPACE, Clock.systemUTC());
    }

    @AfterAll
    static void closeDatabase() {
        database.close();
    }

    @Test
    void concurrentWritersOfOneStoreNeverMeetSqliteBusy() throws Exception {
        try (var pool = SqliteTestSupport.pool(
                        SqliteTestSupport.url(database.file(), "journal_mode=WAL&busy_timeout=1"), true);
                var executor = Executors.newFixedThreadPool(WRITERS)) {
            var store = SqliteTaskStore.builder()
                    .dataSource(pool)
                    .namespace(SqliteTestSupport.prefix(NAMESPACE))
                    .clock(Clock.systemUTC())
                    .build();
            long before = store.idHighWaterMark();
            var start = new CountDownLatch(1);
            var results = new ArrayList<Future<Integer>>();
            for (int w = 0; w < WRITERS; w++) {
                results.add(executor.submit(() -> {
                    start.await();
                    int failed = 0;
                    for (int i = 0; i < WRITES; i++) {
                        try {
                            store.allocateIds(1);
                        } catch (RuntimeException e) {
                            failed++;
                        }
                    }
                    return failed;
                }));
            }
            start.countDown();
            int failed = 0;
            for (var result : results) failed += result.get();

            assertThat(failed).as("writes that failed on SQLite's write lock").isZero();
            assertThat(store.idHighWaterMark() - before).isEqualTo((long) WRITERS * WRITES);
        }
    }
}
