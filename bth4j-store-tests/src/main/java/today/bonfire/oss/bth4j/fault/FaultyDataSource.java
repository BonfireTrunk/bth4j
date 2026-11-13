package today.bonfire.oss.bth4j.fault;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * A {@link DataSource} that cuts connections at the points a {@link FaultPlan} chooses. A point is a round trip
 * to the database: each {@code execute} call of a statement and each {@code commit}. A cut aborts the real
 * connection, so that the transaction open on it is rolled back by the server, and throws a connection failure
 * ({@link SQLException} with SQL state 08006). With {@link FaultPlan.Cut#AFTER} the round trip was made first, so
 * a statement outside a transaction or a commit has taken effect when the caller sees the failure.
 * <p>
 * Wrap the data source of the driver, not a pool, and put the pool on top: the pool then sees the failure as it
 * sees a real one and drops the connection. Neither the PostgreSQL driver nor HikariCP repeats a failed
 * statement, so each cut reaches the caller once.
 */
public final class FaultyDataSource {
    private FaultyDataSource() {}

    public static DataSource around(DataSource real, FaultPlan plan) {
        return (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class}, (proxy, m, args) -> {
                    var result = call(real, m, args);
                    return m.getName().equals("getConnection") ? connection((Connection) result, plan) : result;
                });
    }

    private static Connection connection(Connection real, FaultPlan plan) {
        var self = new Connection[1];
        self[0] = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, m, args) -> {
                    if (m.getName().equals("commit")) return point(real, real, plan, m, args, "commit");
                    var result = call(real, m, args);
                    if (result instanceof CallableStatement s)
                        return statement(self[0], real, s, plan, CallableStatement.class);
                    if (result instanceof PreparedStatement s)
                        return statement(self[0], real, s, plan, PreparedStatement.class);
                    if (result instanceof Statement s) return statement(self[0], real, s, plan, Statement.class);
                    return result;
                });
        return self[0];
    }

    private static Object statement(
            Connection wrapper, Connection real, Statement statement, FaultPlan plan, Class<? extends Statement> type) {
        return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, m, args) -> {
            var name = m.getName();
            if (name.startsWith("execute")) return point(statement, real, plan, m, args, name);
            if (name.equals("getConnection")) return wrapper;
            return call(statement, m, args);
        });
    }

    private static Object point(Object target, Connection real, FaultPlan plan, Method m, Object[] args, String what)
            throws Throwable {
        var cut = plan.reach();
        if (cut == FaultPlan.Cut.BEFORE) {
            abort(real);
            throw new SQLException("injected fault before " + what, "08006");
        }
        var result = call(target, m, args);
        if (cut == FaultPlan.Cut.AFTER) {
            abort(real);
            throw new SQLException("injected fault after " + what, "08006");
        }
        return result;
    }

    /** Drops the real connection, so that the server rolls back what is open on it. */
    private static void abort(Connection real) {
        try {
            real.abort(Runnable::run);
        } catch (SQLException | RuntimeException ignored) {
            // the driver may not support it; closing below drops the connection
        }
        try {
            real.close();
        } catch (SQLException | RuntimeException ignored) {
            // the connection is gone either way
        }
    }

    private static Object call(Object target, Method m, Object[] args) throws Throwable {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
