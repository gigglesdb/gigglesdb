// Single Java 21 process: credentials only arrive through PG* environment variables.
import com.zaxxer.hikari.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.logging.*;

class AgentJdbc {
    static HikariDataSource pool;
    static Connection held;
    static String url;
    static Properties props = new Properties();
    static long statements;
    static void reply(String line) { System.out.println(line); System.out.flush(); }
    static String state(Throwable e) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        String fallback = "null";
        for (; e != null && seen.add(e); e = e.getCause()) {
            if (e instanceof SQLException s) {
                String code = s.getSQLState();
                if (code != null && code.matches("[A-Z0-9]{5}")) {
                    if (code.startsWith("NY")) return code;
                    fallback = code;
                }
            }
        }
        return fallback;
    }
    static String read(Connection c) throws SQLException {
        statements++;
        try (var s = c.prepareStatement("SELECT email FROM customers WHERE id = 3"); var r = s.executeQuery()) {
            if (!r.next()) throw new SQLException();
            String token = r.getString(1);
            if (token == null || !token.startsWith("tok_email_") || r.next()) throw new SQLException();
            return token;
        }
    }
    static void cycle() throws Exception {
        long first = System.currentTimeMillis(), before = statements;
        for (int retries = 0; ; retries++) {
            try {
                String token;
                try (var c = pool.getConnection()) { token = read(c); }
                try (var c = pool.getConnection(); var s = c.prepareStatement("SELECT id FROM customers WHERE email = ?")) {
                    s.setString(1, token);
                    statements++;
                    try (var r = s.executeQuery()) {
                        if (!r.next() || r.getInt(1) != 3 || r.next()) throw new SQLException();
                    }
                }
                String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8))).substring(0,8);
                reply("OK " + hash + " first=" + first + " last=" + System.currentTimeMillis() + " before=" + before + " after=" + statements + " retries=" + retries);
                return;
            } catch (SQLException e) {
                // Hikari validates/evicts stale physical connections itself on the next borrow.
                if (retries >= 2 || state(e).startsWith("NY")) throw e;
                Thread.sleep(250);
            }
        }
    }
    public static void main(String[] args) {
        Logger.getLogger("org.postgresql").setLevel(Level.OFF);
        System.setProperty("slf4j.internal.verbosity", "ERROR");
        boolean ok = false;
        try {
            url = "jdbc:postgresql://" + System.getenv("PGHOST") + ":" + System.getenv("PGPORT") + "/app";
            for (String[] pair : new String[][] {{"user","PGUSER"},{"password","PGPASSWORD"}}) props.setProperty(pair[0], System.getenv(pair[1]));
            props.setProperty("connectTimeout", "10");
            props.setProperty("socketTimeout", "15");
            props.setProperty("stringtype", "unspecified");
            props.setProperty("prepareThreshold", "1");
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url); config.setDataSourceProperties(props);
            config.setMaximumPoolSize(2); config.setMinimumIdle(1);
            config.setKeepaliveTime(0); config.setMaxLifetime(0); config.setIdleTimeout(0);
            config.setConnectionTimeout(20000); config.setValidationTimeout(5000);
            pool = new HikariDataSource(config);
            reply("READY keepalive=" + pool.getKeepaliveTime() + " max=" + pool.getMaximumPoolSize() + " min=" + pool.getMinimumIdle() + " lifetime=" + pool.getMaxLifetime() + " idle=" + pool.getIdleTimeout());
            var input = new BufferedReader(new InputStreamReader(System.in));
            for (String command; (command = input.readLine()) != null;) {
                switch (command) {
                    case "cycle" -> cycle();
                    case "hold" -> { held = pool.getConnection(); read(held); reply("HELD"); }
                    case "refused" -> {
                        String existing = "none", fresh = "none";
                        if (held == null) throw new SQLException();
                        try { read(held); } catch (SQLException e) { existing = state(e); }
                        try (var c = DriverManager.getConnection(url, props)) { } catch (SQLException e) { fresh = state(e); }
                        reply("REFUSED held=" + existing + " fresh=" + fresh);
                        if (!existing.equals("NY011") || !fresh.equals("NY003")) throw new SQLException();
                    }
                    case "quit" -> { ok = true; reply("BYE"); return; }
                    default -> throw new IOException();
                }
            }
        } catch (Exception e) { reply("FAIL state=" + state(e)); }
        finally {
            if (held != null) try { held.close(); } catch (SQLException ignored) { }
            if (pool != null) pool.close();
            if (!ok) System.exit(1);
        }
    }
}
