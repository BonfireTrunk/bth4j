package today.bonfire.oss.bth4j.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;

/** What {@link SqliteTaskStore#init()} requires of the connection and of the file, on real database files. */
@Timeout(60)
class SqliteInitTest {

    @TempDir
    Path directory;

    private final List<HikariDataSource> pools = new ArrayList<>();

    @AfterEach
    void closePools() {
        pools.forEach(HikariDataSource::close);
    }

    private HikariDataSource pool(String parameters) {
        var pool = SqliteTestSupport.pool(SqliteTestSupport.url(directory.resolve("tasks.db"), parameters), true);
        pools.add(pool);
        return pool;
    }

    private SqliteTaskStore store(DataSource dataSource) {
        return SqliteTaskStore.builder()
                .dataSource(dataSource)
                .namespace("app")
                .clock(Clock.systemUTC())
                .build();
    }

    private void sql(DataSource dataSource, String statement) throws SQLException {
        try (var c = dataSource.getConnection();
                var st = c.createStatement()) {
            st.execute(statement);
        }
    }

    private List<String> tables(DataSource dataSource) throws SQLException {
        var names = new ArrayList<String>();
        try (var c = dataSource.getConnection();
                var st = c.createStatement();
                var rs = st.executeQuery("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")) {
            while (rs.next()) names.add(rs.getString(1));
        }
        return names;
    }

    private long scalar(DataSource dataSource, String query) throws SQLException {
        try (var c = dataSource.getConnection();
                var st = c.createStatement();
                var rs = st.executeQuery(query)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void theDriverCarriesASqliteThatTheStoreSupports() throws SQLException {
        var source = pool("journal_mode=WAL&busy_timeout=5000");
        try (var c = source.getConnection();
                var st = c.createStatement();
                var rs = st.executeQuery("SELECT sqlite_version()")) {
            rs.next();
            var parts = rs.getString(1).split("\\.");
            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);

            assertThat(major * 1000 + minor).as("SQLite %s", rs.getString(1)).isGreaterThanOrEqualTo(3 * 1000 + 45);
        }
    }

    @Test
    void initRefusesAConnectionThatIsNotInWalMode() throws SQLException {
        var pool = pool("busy_timeout=5000");

        assertThatThrownBy(() -> store(pool).init())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageContaining("journal_mode is delete")
                .hasMessageContaining("SQLiteConfig.setJournalMode(SQLiteConfig.JournalMode.WAL)");
        assertThat(tables(pool)).isEmpty();
    }

    @Test
    void initRefusesABusyTimeoutBelowOneSecond() throws SQLException {
        for (var timeout : new String[] {"0", "1", "999"}) {
            var pool = pool("journal_mode=WAL&busy_timeout=" + timeout);

            assertThatThrownBy(() -> store(pool).init())
                    .as("busy_timeout %s", timeout)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("busy_timeout is " + timeout + " ms")
                    .hasMessageContaining("at least 1000 ms")
                    .hasMessageContaining("SQLiteConfig.setBusyTimeout(");
            assertThat(tables(pool)).isEmpty();
        }
    }

    @Test
    void initAcceptsExactlyTheSettingsTheRefusalNames() throws SQLException {
        var config = new SQLiteConfig();
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        config.setBusyTimeout(SqliteTaskStore.MIN_BUSY_TIMEOUT_MS);
        var dataSource = new SQLiteDataSource(config);
        dataSource.setUrl(SqliteTestSupport.url(directory.resolve("tasks.db"), ""));

        assertThatCode(() -> store(dataSource).init()).doesNotThrowAnyException();
        assertThat(tables(dataSource)).contains("app_task", "app_meta");
    }

    @Test
    void initCreatesTheSchemaAndTheCountersOnce() throws SQLException {
        var pool = pool("journal_mode=WAL&busy_timeout=5000");
        var store = store(pool);

        store.init();
        store.init();

        assertThat(tables(pool))
                .containsExactly("app_meta", "app_queue", "app_schedule", "app_task", "app_type", "app_worker");
        assertThat(scalar(pool, "SELECT value FROM app_meta WHERE key = 'schema_version'"))
                .isEqualTo(SqliteTaskStore.SCHEMA_VERSION);
        assertThat(scalar(pool, "SELECT count(*) FROM app_meta")).isEqualTo(6);
        assertThat(store.idHighWaterMark()).isEqualTo(1);
    }

    @Test
    void initRefusesAnotherSchemaVersionAndChangesNothing() throws SQLException {
        var pool = pool("journal_mode=WAL&busy_timeout=5000");
        store(pool).init();
        sql(pool, "UPDATE app_meta SET value = 99 WHERE key = 'schema_version'");

        assertThatThrownBy(() -> store(pool).init())
                .isInstanceOf(TaskStoreException.class)
                .hasMessage("namespace app has schema version 99, this library needs " + SqliteTaskStore.SCHEMA_VERSION
                        + " and does not migrate another one");
        assertThat(scalar(pool, "SELECT value FROM app_meta WHERE key = 'schema_version'"))
                .isEqualTo(99);
    }

    @Test
    void initRefusesAMetaTableWithoutAVersion() throws SQLException {
        var pool = pool("journal_mode=WAL&busy_timeout=5000");
        store(pool).init();
        sql(pool, "DELETE FROM app_meta WHERE key = 'schema_version'");

        assertThatThrownBy(() -> store(pool).init())
                .isInstanceOf(TaskStoreException.class)
                .hasMessage("namespace app has schema version null, this library needs "
                        + SqliteTaskStore.SCHEMA_VERSION + " and does not migrate another one");
    }

    @Test
    void initRefusesATaskTableWithoutMeta() throws SQLException {
        var pool = pool("journal_mode=WAL&busy_timeout=5000");
        sql(pool, "CREATE TABLE app_task (id INTEGER PRIMARY KEY)");

        assertThatThrownBy(() -> store(pool).init())
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("app_task exists without app_meta");
        assertThat(tables(pool)).containsExactly("app_task");
    }

    @Test
    void initOnAMatchingSchemaRunsNoDdl() throws SQLException {
        var writable = pool("journal_mode=WAL&busy_timeout=5000");
        store(writable).init();

        var readOnly = new SQLiteConfig();
        readOnly.setReadOnly(true);
        readOnly.setBusyTimeout(SqliteTaskStore.MIN_BUSY_TIMEOUT_MS);
        var dataSource = new SQLiteDataSource(readOnly);
        dataSource.setUrl(SqliteTestSupport.url(directory.resolve("tasks.db"), ""));

        assertThatThrownBy(() -> sql(dataSource, "CREATE TABLE other (x INTEGER)"))
                .isInstanceOf(SQLException.class);
        assertThatCode(() -> store(dataSource).init()).doesNotThrowAnyException();
    }

    @Test
    void initOnAMatchingSchemaDoesNotWaitForTheWriteLock() throws SQLException {
        var holder = pool("journal_mode=WAL&busy_timeout=5000");
        store(holder).init();
        var starter = pool("journal_mode=WAL&busy_timeout=" + SqliteTaskStore.MIN_BUSY_TIMEOUT_MS);

        try (var c = holder.getConnection();
                var st = c.createStatement()) {
            st.execute("BEGIN IMMEDIATE");
            try {
                assertThatCode(() -> store(starter).init()).doesNotThrowAnyException();
            } finally {
                st.execute("ROLLBACK");
            }
        }
    }

    @Test
    void storesInitializingOneFreshFileAtTheSameTimeAllSucceed() throws Exception {
        int stores = 4;
        var executor = Executors.newFixedThreadPool(stores);
        try {
            var start = new CountDownLatch(1);
            var results = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < stores; i++) {
                var pool = pool("journal_mode=WAL&busy_timeout=5000");
                results.add(executor.submit(() -> {
                    start.await();
                    store(pool).init();
                    return null;
                }));
            }
            start.countDown();
            for (var result : results) result.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        var pool = pool("journal_mode=WAL&busy_timeout=5000");
        assertThat(tables(pool)).hasSize(6);
        assertThat(scalar(pool, "SELECT count(*) FROM app_meta")).isEqualTo(6);
    }
}
