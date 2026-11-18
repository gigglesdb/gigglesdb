// Single-file JDBC launcher: fixed labels only, no returned values or diagnostics.
import java.sql.*;
import java.util.*;
import java.io.*;
import java.util.logging.*;
class FencingCheck {
    public static void main(String[] args) {
        Logger.getLogger("").setLevel(Level.OFF);
        String scenario = System.getenv("FENCING_SCENARIO");
        int count = 0;
        boolean ok = false;
        Properties p = new Properties();
        p.setProperty("user", System.getenv("PGUSER"));
        p.setProperty("password", System.getenv("PGPASSWORD"));
        p.setProperty("sslmode", "disable");
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://" + System.getenv("PGHOST") + ":" + System.getenv("PGPORT") + "/" + System.getenv("PGDATABASE"), p)) {
            c.setAutoCommit(false);
            try (PreparedStatement s = c.prepareStatement("SELECT id, secret FROM " + System.getenv("FENCING_TABLE") + " ORDER BY id")) {
                s.setFetchSize(10);
                try (ResultSet r = s.executeQuery()) {
                    while (r.next()) {
                        count++;
                        if (scenario.equals("loosen") && !r.getString(2).equals("MASKED")) throw new IllegalStateException();
                        if (count == 20) {
                            System.out.println("READY"); System.out.flush();
                            if (!new BufferedReader(new InputStreamReader(System.in)).readLine().startsWith("CONTINUE ")) System.exit(1);
                        }
                    }
                }
            }
            ok = !scenario.equals("tighten") && count == 1000;
        } catch (SQLException e) {
            ok = scenario.equals("tighten") && "NY008".equals(e.getSQLState()) && count == 20;
        } catch (Exception e) { ok = false; }
        System.out.println((ok ? "PASS" : "FAIL") + " pgjdbc-portal-" + scenario);
        System.exit(ok ? 0 : 1);
    }
}
