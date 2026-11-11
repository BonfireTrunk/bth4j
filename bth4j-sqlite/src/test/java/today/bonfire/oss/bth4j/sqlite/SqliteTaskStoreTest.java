package today.bonfire.oss.bth4j.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

class SqliteTaskStoreTest {

    private static final DataSource DATA_SOURCE = mock(DataSource.class);

    @Test
    void nullDataSourceAndNullClockAreRejected() {
        assertThatThrownBy(() -> new SqliteTaskStore(null, "bth")).isInstanceOf(TaskConfigurationException.class);
        assertThatThrownBy(() -> new SqliteTaskStore(DATA_SOURCE, "bth", null))
                .isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void prefixMustBeAPlainLowerCaseIdentifier() {
        for (var bad : new String[] {null, "", "Bth", "1bth", "bth-tasks", "bth tasks", "a".repeat(42)}) {
            assertThatThrownBy(() -> new SqliteTaskStore(DATA_SOURCE, bad))
                    .as("prefix %s", bad)
                    .isInstanceOf(TaskConfigurationException.class);
        }
        assertThatCode(() -> new SqliteTaskStore(DATA_SOURCE, "bth_2")).doesNotThrowAnyException();
        assertThatCode(() -> new SqliteTaskStore(DATA_SOURCE, "a".repeat(41), Clock.systemUTC()))
                .doesNotThrowAnyException();
    }

    @Test
    void prefixesThatWouldMakeTableNamesSqliteReservesAreRejected() {
        for (var reserved : new String[] {"sqlite", "sqlite_", "sqlite_app"}) {
            assertThatThrownBy(() -> new SqliteTaskStore(DATA_SOURCE, reserved))
                    .as("prefix %s", reserved)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("reserves");
        }
        assertThatCode(() -> new SqliteTaskStore(DATA_SOURCE, "sqlitex")).doesNotThrowAnyException();
        assertThatCode(() -> new SqliteTaskStore(DATA_SOURCE, "my_sqlite")).doesNotThrowAnyException();
    }

    @Test
    void infoNamesTheEngineAndTheSchemaVersion() {
        var info = new SqliteTaskStore(DATA_SOURCE, "app").info();

        assertThat(info.engine()).isEqualTo("sqlite");
        assertThat(info.schemaVersion())
                .isEqualTo(SqliteTaskStore.SCHEMA_VERSION)
                .isEqualTo(2);
    }

    @Test
    void schemaStatementsUseThePrefixAndTheHouseRules() {
        var statements = new SqliteTaskStore(DATA_SOURCE, "app").schemaStatements();

        assertThat(statements).hasSize(17);
        assertThat(statements.getFirst()).startsWith("CREATE TABLE IF NOT EXISTS app_meta (");
        assertThat(statements).allSatisfy(s -> assertThat(s).contains("app_"));
        assertThat(statements)
                .noneSatisfy(s ->
                        assertThat(s).matches("(?is).*\\b(REFERENCES|CHECK|DEFAULT|TRIGGER|ALTER|AUTOINCREMENT)\\b.*"));
        assertThat(statements)
                .filteredOn(s -> s.startsWith("CREATE TABLE"))
                .hasSize(6)
                .allSatisfy(s -> assertThat(s).endsWith(") STRICT"));
        assertThat(statements)
                .contains(
                        "CREATE TABLE IF NOT EXISTS app_type (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT",
                        "CREATE INDEX IF NOT EXISTS app_task_queued ON app_task (queue_id, due, id, type_id) WHERE state = 0",
                        "CREATE UNIQUE INDEX IF NOT EXISTS app_task_unique_key ON app_task (unique_key)"
                                + " WHERE unique_key IS NOT NULL");
        assertThat(statements)
                .filteredOn(s -> s.startsWith("CREATE TABLE IF NOT EXISTS app_worker"))
                .singleElement()
                .asString()
                .contains("accepting INTEGER NOT NULL", "queue_ids TEXT NOT NULL", "type_ids TEXT NOT NULL");
        assertThat(statements)
                .filteredOn(s -> s.startsWith("CREATE TABLE IF NOT EXISTS app_task"))
                .singleElement()
                .asString()
                .contains("created INTEGER NOT NULL", "due INTEGER NOT NULL", "headers TEXT", "failures TEXT");
        assertThat(statements.getLast()).startsWith("INSERT INTO app_meta").contains("'schema_version', 2");
    }

    @Test
    void emptyClaimsDoNotOpenAConnection() {
        var dataSource = mock(DataSource.class);
        var store = new SqliteTaskStore(dataSource, "app");
        var grants = List.of(new QueueGrant(1, 1, 2));

        assertThat(store.claim(new ClaimRequest(grants, 0, Duration.ofSeconds(30), 1, null)))
                .isEmpty();
        assertThat(store.claim(new ClaimRequest(grants, 1, Duration.ofSeconds(30), 1, Set.of())))
                .isEmpty();
        verifyNoInteractions(dataSource);
    }

    /** A connection whose statements are recorded in {@code statement} and whose prepared statements fail. */
    private static Connection connectionFailingWith(Statement statement, Throwable failure, boolean autoCommit)
            throws SQLException {
        var connection = mock(Connection.class);
        when(connection.getAutoCommit()).thenReturn(autoCommit);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenThrow(failure);
        return connection;
    }

    private static SqliteTaskStore storeOn(Connection connection) throws SQLException {
        var dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);
        return new SqliteTaskStore(dataSource, "app");
    }

    @Test
    void anErrorInsideAWriteRollsBackTheTransactionItStarted() throws SQLException {
        var error = new AssertionError("injected");
        var statement = mock(Statement.class);
        var connection = connectionFailingWith(statement, error, true);

        assertThatThrownBy(() -> storeOn(connection).ack(1, 1, false, false, null))
                .isSameAs(error);

        var order = inOrder(statement);
        order.verify(statement).execute("BEGIN IMMEDIATE");
        order.verify(statement).execute("ROLLBACK");
        verify(statement, never()).execute("COMMIT");
        verify(connection, never()).setAutoCommit(anyBoolean());
    }

    @Test
    void aConnectionWithoutAutoCommitIsSwitchedOnForTheWriteAndOffAgainAfterARollback() throws SQLException {
        var error = new AssertionError("injected");
        var statement = mock(Statement.class);
        var connection = connectionFailingWith(statement, error, false);

        assertThatThrownBy(() -> storeOn(connection).ack(1, 1, false, false, null))
                .isSameAs(error);

        var order = inOrder(connection, statement);
        order.verify(connection).setAutoCommit(true);
        order.verify(statement).execute("BEGIN IMMEDIATE");
        order.verify(statement).execute("ROLLBACK");
        order.verify(connection).setAutoCommit(false);
        verify(statement, never()).execute("COMMIT");
    }

    @Test
    void aFailedRollbackKeepsTheFailureAndAddsItsOwn() throws SQLException {
        var error = new AssertionError("injected");
        var rollback = new SQLException("connection lost");
        var statement = mock(Statement.class);
        doThrow(rollback).when(statement).execute("ROLLBACK");
        var connection = connectionFailingWith(statement, error, true);

        assertThatThrownBy(() -> storeOn(connection).ack(1, 1, false, false, null))
                .isSameAs(error)
                .hasSuppressedException(rollback);
    }

    @Test
    void aWriteCommitsOnceAndGivesTheConnectionBackWithAutoCommitOff() throws SQLException {
        var statement = mock(Statement.class);
        var prepared = mock(PreparedStatement.class);
        when(prepared.executeUpdate()).thenReturn(1);
        var connection = mock(Connection.class);
        when(connection.getAutoCommit()).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(prepared);

        assertThat(storeOn(connection).ack(1, 1, false, false, null)).isTrue();

        var order = inOrder(connection, statement);
        order.verify(connection).setAutoCommit(true);
        order.verify(statement).execute("BEGIN IMMEDIATE");
        order.verify(statement).execute("COMMIT");
        order.verify(connection).setAutoCommit(false);
        verify(statement, never()).execute("ROLLBACK");
        verify(connection, never()).commit();
        verify(connection, never()).rollback();
    }

    @Test
    void aFailedCommitRollsBack() throws SQLException {
        var busy = new SQLException("database is locked");
        var statement = mock(Statement.class);
        doThrow(busy).when(statement).execute("COMMIT");
        var prepared = mock(PreparedStatement.class);
        when(prepared.executeUpdate()).thenReturn(1);
        var connection = mock(Connection.class);
        when(connection.getAutoCommit()).thenReturn(true);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(prepared);

        assertThatThrownBy(() -> storeOn(connection).ack(1, 1, false, false, null))
                .hasCauseReference(busy);

        var order = inOrder(statement);
        order.verify(statement).execute("COMMIT");
        order.verify(statement).execute("ROLLBACK");
    }

    @Test
    void aSqlFailureBecomesATaskStoreException() throws SQLException {
        var failure = new SQLException("disk I/O error");
        var statement = mock(Statement.class);
        var connection = connectionFailingWith(statement, failure, true);

        assertThatThrownBy(() -> storeOn(connection).ack(1, 1, false, false, null))
                .isInstanceOf(TaskStoreException.class)
                .hasCauseReference(failure);
    }
}
