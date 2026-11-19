// One Hikari pool and leased connection survive every policy phase.
import com.zaxxer.hikari.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.logging.*;

class PolicyJdbc {
    static void reply(String text) { System.out.println(text); System.out.flush(); }
    static String cell(String text) {
        return text == null ? "~" : Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.postgresql").setLevel(Level.OFF);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + System.getenv("PGHOST") + ":" + System.getenv("PGPORT") + "/app");
        config.setUsername(System.getenv("PGUSER")); config.setPassword(System.getenv("PGPASSWORD"));
        config.setMaximumPoolSize(2); config.setMinimumIdle(1);
        config.setKeepaliveTime(0); config.setMaxLifetime(0); config.setIdleTimeout(0);
        config.addDataSourceProperty("prepareThreshold", "1");
        config.addDataSourceProperty("socketTimeout", "30");
        try (var pool = new HikariDataSource(config); var input = new BufferedReader(new InputStreamReader(System.in))) {
            Connection conn = pool.getConnection();
            Statement stream = null; ResultSet rows = null;
            reply("READY");
            for (String line; (line = input.readLine()) != null;) {
                if (line.equals("quit")) { conn.close(); reply("BYE"); return; }
                try {
                    if (line.equals("reconnect")) { conn.close(); pool.getHikariPoolMXBean().softEvictConnections(); conn = pool.getConnection(); reply("OK"); continue; }
                    if (line.equals("stream")) {
                        conn.setAutoCommit(false);
                        stream = conn.createStatement(); stream.setFetchSize(1);
                        rows = stream.executeQuery("SELECT id, body FROM notes ORDER BY id");
                        if (!rows.next()) throw new SQLException();
                        reply("READY"); continue;
                    }
                    if (line.equals("drain")) {
                        try { while (rows.next()) { } reply("END"); }
                        finally { rows.close(); stream.close(); conn.rollback(); conn.setAutoCommit(true); }
                        continue;
                    }
                    String sql = new String(Base64.getDecoder().decode(line), StandardCharsets.UTF_8);
                    try (var stmt = conn.prepareStatement(sql); var result = stmt.executeQuery()) {
                        while (result.next()) {
                            var cells = new ArrayList<String>();
                            for (int i=1; i<=result.getMetaData().getColumnCount(); i++) cells.add(cell(result.getString(i)));
                            reply("ROW " + String.join(" ", cells));
                        }
                    }
                    reply("OK");
                } catch (SQLException error) {
                    reply("CODE " + error.getSQLState());
                    try { conn.rollback(); conn.setAutoCommit(true); } catch (SQLException ignored) { }
                }
            }
            conn.close();
        }
    }
}
