// Single-file Java 21 launcher. PG* environment variables supply session credentials.
// Never print returned values, credentials, tokens, or exception messages.
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

class jdbc_check {
    static String url;
    static final Properties properties = new Properties();
    static HikariDataSource pool;
    static String detail = "";
    static Connection connectionC;
    static String token;
    static int passed, failures;
    static final BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
    interface Check { boolean run() throws Exception; }

    static List<List<String>> query(Connection connection, String sql, String... params) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) statement.setString(i + 1, params[i]);
            try (ResultSet result = statement.executeQuery()) {
                List<List<String>> rows = new ArrayList<>();
                while (result.next()) {
                    List<String> row = new ArrayList<>();
                    for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) row.add(result.getString(i));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    static class Lease implements AutoCloseable {
        final Connection connection;
        Lease(Connection connection) { this.connection = connection; }
        /** Return the connection to the pool. */
        public void close() throws SQLException { connection.close(); }
    }

    static Lease borrow(int slot) throws SQLException {
        return new Lease(pool.getConnection());
    }

    static String sqlState(Throwable error) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = error; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof SQLException sqlError && sqlError.getSQLState() != null) {
                String state = sqlError.getSQLState();
                return state.matches("[A-Z0-9]{5}") ? state : "null";
            }
        }
        return "null";
    }

    static List<List<String>> pooledQuery(String sql, String... params) throws SQLException {
        try (Lease lease = borrow(0)) { return query(lease.connection, sql, params); }
    }

    static int update(Connection connection, String sql, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, value);
            return statement.executeUpdate();
        }
    }

    static int pooledUpdate(String sql, String value) throws SQLException {
        try (Lease lease = borrow(0)) { return update(lease.connection, sql, value); }
    }

    static boolean refused(Check action, String expected) throws Exception {
        try { action.run(); }
        catch (SQLException error) {
            String state = sqlState(error);
            detail = " (observed SQLSTATE " + state + ")";
            return expected.equals(state);
        }
        detail = " (observed SQLSTATE null)";
        return false;
    }

    static void step(String name, Check check) {
        boolean ok;
        detail = "";
        try { ok = check.run(); } catch (Exception error) { ok = false; }
        System.out.println((ok ? "PASS" : "FAIL") + " step " + (passed + failures + 1) + ": " + name +
            detail);
        System.out.flush();
        if (ok) passed++; else failures++;
    }

    static boolean waitFor(String event) throws Exception {
        System.out.println("WAIT " + event);
        System.out.flush();
        return "GO".equals(input.readLine());
    }

    static boolean openC() throws SQLException {
        connectionC = DriverManager.getConnection(url, properties);
        connectionC.setAutoCommit(true);
        return query(connectionC, "SELECT country FROM customers WHERE id = 3").equals(List.of(List.of("US")));
    }

    static void endSession(String event) throws Exception {
        boolean ready = waitFor(event);
        step(event + " ends open connection (NY011)", () -> ready && refused(() -> {
            query(connectionC, "SELECT country FROM customers WHERE id = 3"); return true;
        }, "NY011"));
        step(event + " refuses new connection (NY003 expected)", () -> ready && refused(() -> {
            try (Connection ignored = DriverManager.getConnection(url, properties)) { return true; }
        }, "NY003"));
    }

    static void mainChecks() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setDataSourceProperties(properties);
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(2);
        config.setAutoCommit(true);
        config.setConnectionTimeout(30000);
        config.setValidationTimeout(5000);
        step("HikariCP pool starts", () -> {
            try {
                pool = new HikariDataSource(config);
                return true;
            } catch (Exception error) {
                detail = " (observed SQLSTATE " + sqlState(error) + ")";
                return false;
            }
        });
        if (pool == null) return;
        try {
            step("read blind emails as tokens", () -> {
                var rows = pooledQuery("SELECT id, email FROM customers WHERE failed_payments >= 2 ORDER BY id");
                if (rows.size() != 2 || !"3".equals(rows.get(0).get(0)) || !"5".equals(rows.get(1).get(0))) return false;
                for (var row : rows) if (row.get(1) == null || !row.get(1).startsWith("tok_email_")) return false;
                token = rows.get(0).get(1);
                return true;
            });
            step("write token to same domain", () -> token != null && pooledUpdate(
                "INSERT INTO outbox (to_address, template) VALUES (?, 'reminder')", token) == 1);
            step("read same-domain token unchanged", () -> token != null &&
                pooledQuery("SELECT to_address FROM outbox").contains(List.of(token)));
            step("refuse token into plain notes (NY001)", () -> token != null && refused(() ->
                pooledUpdate("UPDATE notes SET body = ? WHERE id = 3", token) == 1, "NY001"));
            step("refuse blind LIKE (NY002)", () -> refused(() -> {
                pooledQuery("SELECT id FROM customers WHERE email LIKE 'a%'"); return true;
            }, "NY002"));
            step("plain country query for absent id", () ->
                pooledQuery("SELECT country FROM customers WHERE id = 17").isEmpty());
            step("mask phone output", () -> pooledQuery("SELECT phone FROM customers WHERE id = 3")
                .equals(List.of(List.of("[hidden]"))));
            step("pooled token round trip on distinct connections", () -> {
                try (Lease leaseA = borrow(0)) {
                    Connection a = leaseA.connection;
                    var rows = query(a, "SELECT email FROM customers WHERE id = 3");
                    if (rows.size() != 1 || rows.get(0).get(0) == null ||
                        !rows.get(0).get(0).startsWith("tok_email_")) return false;
                    token = rows.get(0).get(0);
                    try (Lease leaseB = borrow(1)) {
                        Connection b = leaseB.connection;
                        int inserted = update(b, "INSERT INTO outbox (to_address, template) VALUES (?, 'pooled')", token);
                        var returned = query(b, "SELECT to_address FROM outbox WHERE to_address = ?", token);
                        return a != b && inserted == 1 && !returned.isEmpty() &&
                            returned.stream().allMatch(row -> row.equals(List.of(token)));
                    }
                }
            });
            step("same token after gateway restart and pool recovery", () -> {
                if (!waitFor("restart") || token == null) return false;
                // Validate both old physical connections, replacing any broken one before the read.
                try (Connection a = pool.getConnection(); Connection b = pool.getConnection()) {
                    if (!a.isValid(5)) pool.evictConnection(a);
                    if (!b.isValid(5)) pool.evictConnection(b);
                }
                return pooledQuery("SELECT email FROM customers WHERE id = 3").equals(List.of(List.of(token)));
            });
            step("open revocation connection", jdbc_check::openC);
            endSession("revoke");
        } finally {
            if (pool != null) pool.close();
        }
    }

    public static void main(String[] args) {
        // Disable diagnostics before pgJDBC/Hikari initialize; they can contain server data.
        Logger.getLogger("org.postgresql").setLevel(Level.OFF);
        System.setProperty("slf4j.internal.verbosity", "ERROR");
        try {
            if (args.length != 1 || !(args[0].equals("main") || args[0].equals("expiry"))) throw new Exception();
            url = "jdbc:postgresql://" + System.getenv("PGHOST") + ":" + System.getenv("PGPORT") + "/app";
            properties.setProperty("user", System.getenv("PGUSER"));
            properties.setProperty("password", System.getenv("PGPASSWORD"));
            properties.setProperty("preferQueryMode", "extended");
            properties.setProperty("prepareThreshold", "1");
            properties.setProperty("binaryTransfer", "false");
            properties.setProperty("stringtype", "unspecified");
            properties.setProperty("connectTimeout", "10");
            if (args[0].equals("expiry")) {
                step("open expiry connection", jdbc_check::openC);
                endSession("expiry");
            } else mainChecks();
        } catch (Exception error) {
            String name = "connection/setup";
            for (Throwable cause = error; cause != null; cause = cause.getCause()) {
                name += " " + cause.getClass().getSimpleName();
                if (cause instanceof SQLException sqlError) {
                    String state = sqlError.getSQLState();
                    name += " SQLSTATE " + (state != null && state.matches("[A-Z0-9]{5}") ? state : "null");
                }
            }
            step(name, () -> false);
        } finally {
            if (connectionC != null) try { connectionC.close(); } catch (SQLException ignored) { }
        }
        System.out.println(passed + " passed; " + failures + " failed");
        System.exit(failures == 0 ? 0 : 1);
    }
}
