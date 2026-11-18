// Single-file Java launcher; access-session connection settings come from PG*.
// Never print returned values, credentials, tokens, or exception messages.
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class DefaultsCheck {
    static final Pattern GENERATED_TOKEN = Pattern.compile("tok_(f[a-z2-7]{10})_([a-z2-7]+)");
    static final Pattern TOKEN = Pattern.compile("tok_(.+)_([a-z2-7]+)");
    static final Properties properties = new Properties();
    static final Set<String> declared = new HashSet<>(Arrays.asList(
        System.getenv().getOrDefault("NUNYA_TEST_DEFAULTS_DECLARED_DOMAINS", "email").split(",")));
    static Connection connection;
    static String url;
    static String token;
    static int passed, failed;
    interface Check { boolean run() throws Exception; }

    static void step(String name, Check action) {
        boolean ok;
        try { ok = action.run(); } catch (Exception error) { ok = false; }
        System.out.println((ok ? "PASS" : "FAIL") + " step " + (passed + failed + 1) + ": " + name);
        System.out.flush();
        if (ok) passed++; else failed++;
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet result = statement.executeQuery()) {
            List<List<String>> rows = new ArrayList<>();
            while (result.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    row.add(result.getString(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }

    static boolean directToken() throws SQLException {
        connection = DriverManager.getConnection(url, properties);
        connection.setAutoCommit(true);
        var rows = query("SELECT recovery_email, email FROM customers WHERE id = 1");
        if (rows.size() != 1 || rows.get(0).size() != 2) return false;
        String recovery = rows.get(0).get(0);
        String email = rows.get(0).get(1);
        if (recovery == null || email == null) return false;
        Matcher generated = GENERATED_TOKEN.matcher(recovery);
        Matcher emailToken = TOKEN.matcher(email);
        if (!generated.matches() || !emailToken.matches() ||
            declared.contains(generated.group(1)) || generated.group(1).equals(emailToken.group(1))) return false;
        token = recovery;
        return true;
    }

    static boolean starToken() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM customers WHERE id = 1");
             ResultSet result = statement.executeQuery()) {
            if (token == null || !result.next()) return false;
            String recovered = result.getString("recovery_email");
            return token.equals(recovered) && !result.next();
        }
    }

    static boolean isNull(String column) throws SQLException {
        var rows = query("SELECT " + column + " FROM customers WHERE id = 1");
        return rows.size() == 1 && rows.get(0).size() == 1 && rows.get(0).get(0) == null;
    }

    static boolean refused(String sql) throws SQLException {
        try { query(sql); }
        catch (SQLException error) { return "NY005".equals(error.getSQLState()); }
        return false;
    }

    static boolean reconnect() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        connection = DriverManager.getConnection(url, properties);
        connection.setAutoCommit(true);
        return token != null && query("SELECT recovery_email FROM customers WHERE id = 1")
            .equals(List.of(List.of(token)));
    }

    /** Run refreshed route-default checks without disclosing values or diagnostics. */
    public static void main(String[] args) {
        Logger.getLogger("org.postgresql").setLevel(Level.OFF);
        try {
            String host = System.getenv("PGHOST");
            if (host != null && host.contains(":")) host = "[" + host + "]";
            url = "jdbc:postgresql://" + host + ":" + System.getenv("PGPORT") + "/" +
                System.getenv().getOrDefault("PGDATABASE", "app");
            properties.setProperty("user", System.getenv("PGUSER"));
            properties.setProperty("password", System.getenv("PGPASSWORD"));
            properties.setProperty("sslmode", System.getenv().getOrDefault("PGSSLMODE", "disable"));
            properties.setProperty("preferQueryMode", "extended");
            properties.setProperty("prepareThreshold", "1");
            properties.setProperty("binaryTransfer", "false");
            properties.setProperty("stringtype", "unspecified");
            properties.setProperty("connectTimeout", "10");
            step("direct generated token has distinct domain", DefaultsCheck::directToken);
            step("star read returns identical token", DefaultsCheck::starToken);
            step("tier is SQL NULL", () -> isNull("tier"));
            step("active is SQL NULL", () -> isNull("active"));
            step("kind is SQL NULL", () -> isNull("kind"));
            step("tier WHERE refused NY005", () -> refused("SELECT id FROM customers WHERE tier = 1"));
            step("active WHERE refused NY005", () -> refused("SELECT id FROM customers WHERE active"));
            step("kind WHERE refused NY005", () -> refused("SELECT id FROM customers WHERE kind = 'x'"));
            step("reconnect in same access session preserves token", DefaultsCheck::reconnect);
        } catch (Exception error) {
            step("connection setup", () -> false);
        } finally {
            if (connection != null) try { connection.close(); } catch (SQLException ignored) { }
        }
        System.out.println(passed + " passed; " + failed + " failed");
        System.exit(failed == 0 ? 0 : 1);
    }
}
