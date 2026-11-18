// Single-file launcher. Outcomes only: never print data, credentials or exceptions.
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.*;
class JsonPathsCheck {
    static int passed, failed;
    static final String ORIGINAL = "{\"status\":\"sent\",\"recipients\":[{\"email\":\"first@example.test\",\"phone\":\"fixture-phone\"},{\"email\":\"second@example.test\",\"phone\":\"fixture-phone\"}],\"processor_response\":\"fixture-processor\"}";
    static final String CHANGED = "{\"status\":{\"unapproved\":\"fixture-hidden\"},\"recipients\":[{\"email\":\"first@example.test\",\"phone\":\"fixture-phone\"},7,null,\"fixture-hidden\",{}],\"new_top\":\"fixture-top\"}";
    interface Check { boolean run() throws Exception; }
    static void step(String label, Check check) throws Exception {
        boolean ok;
        try { ok = check.run(); } catch (Exception e) { ok = false; }
        System.out.println((ok ? "PASS " : "FAIL ") + label);
        if (ok) passed++; else { failed++; throw new Exception("check failed"); }
    }
    static List<String> read(Connection c) throws Exception {
        try (var s = c.prepareStatement("SELECT doc_json, doc_jsonb FROM events WHERE id = ?")) {
            s.setInt(1, 1);
            try (var r = s.executeQuery()) {
                if (!r.next()) throw new Exception("missing row");
                return List.of(cell(r,1), cell(r,2));
            }
        }
    }
    static String cell(ResultSet r, int col) throws Exception {
        byte[] raw = r.getBytes(col);
        if(raw == null) throw new Exception("unexpected SQL NULL");
        var metadata = (org.postgresql.PGResultSetMetaData)r.getMetaData();
        int skip = metadata.getFormat(col)==1 && col==2 ? 1 : 0;
        if(skip==1 && raw[0]!=1) throw new Exception("invalid JSONB version");
        return new String(raw,skip,raw.length-skip,java.nio.charset.StandardCharsets.UTF_8).replaceAll("\\s+", "");
    }
    static List<String> tokens(String doc) {
        var m = Pattern.compile("\"email\":\"(tok_email_[a-z2-7]+)\"").matcher(doc);
        var out = new ArrayList<String>(); while(m.find()) out.add(m.group(1)); return out;
    }
    static boolean safe(String doc) {
        return !doc.contains("phone") && !doc.contains("processor_response") && !doc.contains("example.test") && !doc.contains("fixture-hidden");
    }
    static void update(Connection admin, String doc) throws Exception {
        String schema = System.getenv("NUNYA_TEST_JSON_SCHEMA");
        if (!schema.matches("json_[0-9a-f]+")) throw new Exception("invalid schema");
        try (var s = admin.prepareStatement("UPDATE " + schema + ".events SET doc_json=?::json, doc_jsonb=?::jsonb WHERE id=1")) {
            s.setString(1,doc); s.setString(2,doc); s.executeUpdate();
        }
    }
    static List<String> query(Connection c, String sql, String param) throws Exception {
        try(var s=c.prepareStatement(sql)) {
            if(param!=null) s.setString(1,param);
            try(var r=s.executeQuery()) { var out=new ArrayList<String>(); while(r.next()) out.add(r.getString(1)); return out; }
        }
    }
    static void operators(Connection c, Connection admin, String label) throws Exception {
        for(String col : new String[]{"doc_json","doc_jsonb"}) {
            String whole=query(c,"SELECT "+col+" FROM events",null).get(0).replaceAll("\\s+", "");
            step(label+" masked", () -> query(c,"SELECT "+col+"->'processor_response' FROM events",null).get(0)==null);
            step(label+" parent", () -> tokens(query(c,"SELECT "+col+"->'recipients' FROM events",null).get(0).replaceAll("\\s+", "")).equals(tokens(whole)));
            String path=col+" #>> '{recipients,0,email}'";
            String token=query(c,"SELECT "+path+" FROM events",null).get(0);
            step(label+" token", () -> tokens(whole).get(0).equals(token));
            step(label+" show-equality", () -> query(c,"SELECT id FROM events WHERE "+col+"->>'status'='sent'",null).size()==1);
            step(label+" show-null", () -> query(c,"SELECT id FROM events WHERE "+col+"->>'status' IS NULL",null).isEmpty());
            step(label+" token-param", () -> query(c,"SELECT id FROM events WHERE "+path+" = ?",token).size()==1);
            step(label+" token-literal", () -> query(c,"SELECT id FROM events WHERE "+path+" = '"+token+"'",null).size()==1);
            step(label+" join", () -> query(c,"SELECT e.id FROM events e JOIN customers c ON e."+path+" = c.email",null).size()==1);
            for(String sql : new String[]{"SELECT id FROM events WHERE "+col+"->>'processor_response' IS NULL", "SELECT id FROM events WHERE "+col+"->>'processor_response'='x'", "SELECT "+col+"->id FROM events", "SELECT doc_jsonb @> '{}' FROM events", "SELECT doc_jsonb ? 'status' FROM events", "SELECT jsonb_path_query(doc_jsonb,'$.status') FROM events", "UPDATE events SET id=id WHERE "+col+"->>'status'='sent'"}) {
                step(label+" refusal-NY005", () -> {try(var st=c.createStatement()) {st.execute(sql); return false;} catch(SQLException e) {return "NY005".equals(e.getSQLState());}});
            }
        }
        update(admin,CHANGED);
        step(label+" shape-projection", () -> query(c,"SELECT doc_jsonb->>'status' FROM events",null).get(0)==null);
        step(label+" shape-predicate", () -> query(c,"SELECT id FROM events WHERE doc_jsonb->>'status'=?","{\"unapproved\": \"fixture-hidden\"}").isEmpty());
        update(admin,ORIGINAL);
    }
    static void showOperators(Connection c, String label) throws Exception {
        for(String col : new String[]{"doc_json","doc_jsonb"}) {
            step(label+" show-parent", () -> query(c,"SELECT "+col+"->'payment' FROM events",null).get(0).replaceAll("\\s+", "").equals("{\"amount\":42}"));
            step(label+" show-new-key", () -> "fixture-top".equals(query(c,"SELECT "+col+"->>'new_top' FROM events",null).get(0)));
            step(label+" show-masked", () -> query(c,"SELECT "+col+" #> '{payment,card}' FROM events",null).get(0)==null);
            step(label+" index-container", () -> query(c,"SELECT "+col+" #> '{items,0,card}' FROM events",null).get(0)==null);
            step(label+" show-refusal", () -> {try(var st=c.createStatement()) {st.execute("SELECT id FROM events WHERE "+col+" #> '{payment,card}' IS NULL"); return false;} catch(SQLException e) {return "NY005".equals(e.getSQLState());}});
        }
    }
    public static void main(String[] args) {
        Logger.getLogger("org.postgresql").setLevel(Level.OFF);
        var props = new Properties(); props.setProperty("user",System.getenv("PGUSER")); props.setProperty("password",System.getenv("PGPASSWORD"));
        props.setProperty("sslmode","disable"); props.setProperty("prepareThreshold","1"); props.setProperty("stringtype","unspecified");
        String url = "jdbc:postgresql://"+System.getenv("PGHOST")+":"+System.getenv("PGPORT")+"/app";
        try (Connection admin = DriverManager.getConnection(System.getenv("NUNYA_TEST_JSON_JDBC_ADMIN"))) {
            for (boolean binary : new boolean[]{false,true}) {
                props.setProperty("binaryTransfer", Boolean.toString(binary));
                if(binary) props.setProperty("binaryTransferEnable","json,jsonb"); else props.remove("binaryTransferEnable");
                String label = "pgjdbc-"+(binary ? "binary" : "text");
                boolean show="show".equals(System.getenv("NUNYA_TEST_JSON_OPERATORS"));
                update(admin, show ? "{\"payment\":{\"card\":\"fixture-hidden\",\"amount\":42},\"new_top\":\"fixture-top\",\"items\":{\"0\":{\"card\":\"fixture-hidden\"}}}" : ORIGINAL);
                List<String> initial;
                try (Connection c = DriverManager.getConnection(url,props)) {
                    if(show) { showOperators(c,label); continue; }
                    if("1".equals(System.getenv("NUNYA_TEST_JSON_OPERATORS"))) { operators(c,admin,label); continue; }
                    initial = read(c);
                    step(label+" token-and-columns", () -> initial.get(0).equals(initial.get(1)) && safe(initial.get(0)) && initial.get(0).contains("\"status\":\"sent\"") && tokens(initial.get(0)).size()==2);
                    update(admin, CHANGED); var changed = read(c);
                    step(label+" shape-change", () -> changed.get(0).equals(changed.get(1)) && safe(changed.get(0)) && !changed.get(0).contains("status") && !changed.get(0).contains("new_top") && changed.get(0).contains(",null,null,null,{}]") && tokens(changed.get(0)).equals(tokens(initial.get(0)).subList(0,1)));
                    update(admin,"{\"status\":null,\"recipients\":[{}]}"); var nul = read(c);
                    step(label+" null-missing", () -> nul.get(0).equals(nul.get(1)) && nul.get(0).contains("\"status\":null") && nul.get(0).contains("\"recipients\":[{}]"));
                    update(admin,ORIGINAL);
                }
                try (Connection c = DriverManager.getConnection(url,props)) { step(label+" reconnect-stable", () -> read(c).equals(initial)); }
            }
            update(admin,ORIGINAL);
        } catch(Exception e) { if(failed==0) {failed++;System.out.println("FAIL pgjdbc-setup-or-update");} }
        System.out.println("SUMMARY pgjdbc passed="+passed+" failed="+failed); System.exit(failed==0 ? 0 : 1);
    }
}