package today.bonfire.oss.bth4j.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;

class PostgresTaskStoreTest {

    private static final DataSource DATA_SOURCE = mock(DataSource.class);

    @Test
    void nullDataSourceIsRejected() {
        assertThatThrownBy(() -> PostgresTaskStore.builder()
                        .dataSource(null)
                        .namespace("bth")
                        .build())
                .isInstanceOf(TaskConfigurationException.class);
    }

    @Test
    void dataSourceAndNamespaceAreRequired() {
        assertThatThrownBy(() -> PostgresTaskStore.builder().namespace("bth").build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessage("DataSource is required");
        assertThatThrownBy(() ->
                        PostgresTaskStore.builder().dataSource(DATA_SOURCE).build())
                .isInstanceOf(TaskConfigurationException.class)
                .hasMessageStartingWith("namespace must match ");
    }

    @Test
    void oneBuilderBuildsTwoStoresThatDoNotShareItsLaterValues() {
        var builder = PostgresTaskStore.builder().dataSource(DATA_SOURCE).namespace("first");
        var first = builder.build();
        var second = builder.namespace("second").build();

        assertThat(first.schemaStatements().getFirst()).startsWith("CREATE TABLE IF NOT EXISTS first_meta (");
        assertThat(second.schemaStatements().getFirst()).startsWith("CREATE TABLE IF NOT EXISTS second_meta (");
    }

    @Test
    void namespaceMustBeAPlainLowerCaseIdentifier() {
        for (var bad : new String[] {null, "", "Bth", "1bth", "bth-tasks", "bth tasks", "a".repeat(42)}) {
            assertThatThrownBy(() -> PostgresTaskStore.builder()
                            .dataSource(DATA_SOURCE)
                            .namespace(bad)
                            .build())
                    .as("namespace %s", bad)
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageStartingWith("namespace must match ");
        }
        assertThatCode(() -> PostgresTaskStore.builder()
                        .dataSource(DATA_SOURCE)
                        .namespace("bth_2")
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    void schemaStatementsUseThePrefix() {
        var statements = PostgresTaskStore.builder()
                .dataSource(DATA_SOURCE)
                .namespace("app")
                .build()
                .schemaStatements();

        assertThat(statements).hasSize(19);
        assertThat(statements.getFirst()).startsWith("CREATE TABLE IF NOT EXISTS app_meta (");
        assertThat(statements).allSatisfy(s -> assertThat(s).contains("app_"));
        assertThat(statements).noneSatisfy(s -> assertThat(s).containsIgnoringCase("REFERENCES"));
        assertThat(statements).noneSatisfy(s -> assertThat(s).containsIgnoringCase("ALTER"));
        assertThat(statements).noneSatisfy(s -> assertThat(s).contains("event"));
        assertThat(statements)
                .contains(
                        "CREATE TABLE IF NOT EXISTS app_type (id smallserial PRIMARY KEY, name text NOT NULL UNIQUE)",
                        "CREATE INDEX IF NOT EXISTS app_task_queued ON app_task (queue_id, due, id, type_id) WHERE state = 0");
        assertThat(statements)
                .filteredOn(s -> s.startsWith("CREATE TABLE IF NOT EXISTS app_worker"))
                .singleElement()
                .asString()
                .contains(
                        "accepting boolean NOT NULL", "queue_ids smallint[] NOT NULL", "type_ids smallint[] NOT NULL");
        assertThat(statements)
                .filteredOn(s -> s.startsWith("CREATE TABLE IF NOT EXISTS app_task")
                        || s.startsWith("CREATE TABLE IF NOT EXISTS app_schedule"))
                .hasSize(2)
                .allSatisfy(s -> assertThat(s).contains("type_id smallint NOT NULL"));
        assertThat(statements.getLast()).startsWith("INSERT INTO app_meta");
    }

    @Test
    void emptyClaimsDoNotOpenAConnection() {
        var dataSource = mock(DataSource.class);
        var store = PostgresTaskStore.builder()
                .dataSource(dataSource)
                .namespace("app")
                .build();
        var grants = List.of(new QueueGrant(1, 1, 2));
        assertThat(store.claim(new ClaimRequest(grants, 0, Duration.ofSeconds(30), 1, null)))
                .isEmpty();
        assertThat(store.claim(new ClaimRequest(grants, 1, Duration.ofSeconds(30), 1, Set.of())))
                .isEmpty();
        verifyNoInteractions(dataSource);
    }

    private static Connection connectionFailingWith(Throwable failure, boolean autoCommit) throws SQLException {
        var connection = mock(Connection.class);
        when(connection.getAutoCommit()).thenReturn(autoCommit);
        when(connection.prepareStatement(anyString())).thenThrow(failure);
        return connection;
    }

    private static PostgresTaskStore storeOn(Connection connection) throws SQLException {
        var dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);
        return PostgresTaskStore.builder()
                .dataSource(dataSource)
                .namespace("app")
                .build();
    }

    @Test
    void anErrorInsideATransactionRollsBackBeforeAutoCommitReturns() throws SQLException {
        var error = new AssertionError("injected");
        var connection = connectionFailingWith(error, true);

        assertThatThrownBy(() -> storeOn(connection).ack(1, 1, false, false, ScheduleEffect.pause("s", 1)))
                .isSameAs(error);

        var order = inOrder(connection);
        order.verify(connection).setAutoCommit(false);
        order.verify(connection).rollback();
        order.verify(connection).setAutoCommit(true);
        verify(connection, never()).commit();
    }

    @Test
    void aFailedRollbackKeepsTheFailureAndLeavesAutoCommitOff() throws SQLException {
        var error = new AssertionError("injected");
        var connection = connectionFailingWith(error, true);
        var rollback = new SQLException("connection lost");
        doThrow(rollback).when(connection).rollback();

        assertThatThrownBy(() -> storeOn(connection).ack(1, 1, false, false, ScheduleEffect.pause("s", 1)))
                .isSameAs(error)
                .hasSuppressedException(rollback);

        verify(connection, never()).setAutoCommit(true);
        verify(connection, never()).commit();
    }

    @Test
    void anErrorOnAConnectionWithoutAutoCommitRollsBack() throws SQLException {
        var error = new AssertionError("injected");
        var connection = connectionFailingWith(error, false);

        assertThatThrownBy(() -> storeOn(connection).purge(TaskState.DEAD, Duration.ZERO, 10))
                .isSameAs(error);

        verify(connection).rollback();
        verify(connection, never()).commit();
    }

    @Test
    void prefixForNamespaceIsLowerCaseAndSafe() {
        assertThat(PostgresTestSupport.prefix("T12")).isEqualTo("t_t12");
        assertThat(PostgresTestSupport.prefix("BTH-1")).isEqualTo("t_bth_1");
    }
}
