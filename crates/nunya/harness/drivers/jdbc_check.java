// Single-file source launcher (Java 17+); supply pgJDBC as check-pg-clients.sh does:
// java --class-path /path/to/postgresql-42.7.3.jar jdbc_check.java JDBC_URL TASK_ID SECRET
// Use a freshly seeded harness. Never print returned values or exception messages.
import java.sql.*;
import java.util.*;

class jdbc_check {
    static Connection connection;
    static String token;
    static int failures;
    interface Check { boolean run() throws Exception; }

    static List<List<String>> query(String sql, String... params) throws SQLException {
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

    static int update(String sql, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, value);
            return statement.executeUpdate();
        }
    }

    static boolean refused(String sql, String value, String state) throws SQLException {
        try {
            if (value == null) query(sql); else update(sql, value);
        } catch (SQLException error) {
            return state.equals(error.getSQLState());
        }
        return false;
    }

    static void step(int number, String name, Check check) {
        boolean ok;
        try { ok = check.run(); } catch (Exception error) { ok = false; }
        System.out.println((ok ? "PASS" : "FAIL") + " step " + number + ": " + name);
        if (!ok) failures++;
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: java --class-path PGJDBC_JAR jdbc_check.java JDBC_URL TASK_ID SECRET");
            System.exit(2);
        }
        Properties properties = new Properties();
        properties.setProperty("user", "task:" + args[1]);
        properties.setProperty("password", args[2]);
        properties.setProperty("preferQueryMode", "extended");
        properties.setProperty("prepareThreshold", "1");
        properties.setProperty("binaryTransfer", "false");
        properties.setProperty("stringtype", "unspecified");
        try (Connection opened = DriverManager.getConnection(args[0], properties)) {
            connection = opened;
            connection.setAutoCommit(true);
            step(1, "read blind emails as tokens", () -> {
                var rows = query("SELECT id, email FROM customers WHERE failed_payments >= 2 ORDER BY id");
                if (rows.size() != 2 || !"3".equals(rows.get(0).get(0)) || !"5".equals(rows.get(1).get(0))) return false;
                for (var row : rows) if (row.get(1) == null || !row.get(1).startsWith("tok_email_")) return false;
                token = rows.get(0).get(1);
                return true;
            });
            step(2, "write token to same domain", () -> token != null && update(
                "INSERT INTO outbox (to_address, template) VALUES (?, 'reminder')", token) == 1);
            step(3, "read same-domain token unchanged", () -> token != null &&
                query("SELECT to_address FROM outbox").contains(List.of(token)));
            step(4, "refuse token into plain notes (NY001)", () -> token != null && refused(
                "UPDATE notes SET body = ? WHERE id = 3", token, "NY001"));
            step(5, "refuse blind LIKE (NY002)", () -> refused(
                "SELECT id FROM customers WHERE email LIKE 'a%'", null, "NY002"));
            // country is plain in harness policy; seed contains no id 17.
            step(6, "plain country query for absent id", () -> query(
                "SELECT country FROM customers WHERE id = 17").isEmpty());
            step(7, "mask phone output", () -> query("SELECT phone FROM customers WHERE id = 3")
                .equals(List.of(List.of("[hidden]"))));
        } catch (Exception error) {
            System.out.println("FAIL connection/setup (details suppressed)");
            System.exit(1);
        }
        System.out.println((7 - failures) + " passed; " + failures + " failed");
        System.exit(failures == 0 ? 0 : 1);
    }
}
