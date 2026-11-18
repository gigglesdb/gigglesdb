// Live shared-domain checks. Never print values, tokens, or exception messages.
import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;
class SharedDomainsCheck {
    static int passed;
    static String url = "jdbc:postgresql://127.0.0.1:" + System.getenv("PGPORT") + "/app?sslmode=disable";
    static void check(String label, boolean ok) {
        System.out.println((ok ? "PASS " : "FAIL ") + label); System.out.flush();
        if (!ok) { System.out.println("SUMMARY pgJDBC passed="+passed+" failed=1"); System.exit(1); }
        passed++;
    }
    static String token(Connection c, String sql) throws Exception {
        try (var s=c.createStatement(); var r=s.executeQuery(sql)) { if (!r.next()) throw new Exception(); return r.getString(1); }
    }
    static boolean refused(Connection c, String sql, String param) throws Exception {
        try (var s=c.prepareStatement(sql)) { if (param != null) s.setString(1,param); s.execute(); return false; }
        catch (SQLException e) { return e.getSQLState()!=null && e.getSQLState().startsWith("NY") && !e.getMessage().contains("example.test"); }
    }
    public static void main(String[] args) {
        Logger.getLogger("org.postgresql").setLevel(Level.OFF);
        try (var c=DriverManager.getConnection(url,System.getenv("PGUSER"),System.getenv("PGPASSWORD"))) {
            String old=token(c,"SELECT email FROM customers WHERE id=1");
            check("before-distinct", !old.equals(token(c,"SELECT to_address FROM outbox WHERE id=2")));
            String join="SELECT customers.id FROM customers JOIN outbox ON customers.email=outbox.to_address";
            check("before-join",refused(c,join,null));
            check("before-write",refused(c,"INSERT INTO outbox(id,to_address) SELECT id,email FROM customers",null));
            System.out.println("READY"); System.out.flush();
            var input=new BufferedReader(new InputStreamReader(System.in));
            String user=input.readLine(), password=input.readLine();
            boolean reset=false; String resetCode=null;
            try (var s=c.createStatement()) { s.executeQuery("SELECT id FROM customers"); }
            catch (SQLException e) { resetCode=e.getSQLState(); reset="NY011".equals(e.getSQLState()) || (e.getSQLState()!=null && e.getSQLState().startsWith("08")); }
            check("reset-NY011-or-closed",reset);
            check("NY011".equals(resetCode) ? "reset-form-NY011" : "reset-form-closed",reset);
            try (var n=DriverManager.getConnection(url,user,password)) {
                check("linked-equal",token(n,"SELECT email FROM customers WHERE id=1").equals(token(n,"SELECT to_address FROM outbox WHERE id=2")));
                try(var s=n.createStatement();var r=s.executeQuery(join)) { check("linked-join",r.next()); }
                check("old-literal",refused(n,"SELECT id FROM customers WHERE email='"+old+"'",null));
                check("old-parameter",refused(n,"SELECT id FROM customers WHERE email=?",old));
            }
            System.out.println("SUMMARY pgJDBC passed="+passed+" failed=0");
        } catch(Exception e) { System.out.println("FAIL driver-operation; SUMMARY pgJDBC passed="+passed+" failed=1"); System.exit(1); }
    }
}
