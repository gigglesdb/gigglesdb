import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Resolve a two-table transaction through another node after killing its gateway. */
public class OutcomeJdbc {
    static Map<?, ?> cluster;
    static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    static double now() { return System.nanoTime() / 1e9; }
    static Map<?, ?> obj(Object o) { return (Map<?, ?>) o; }
    static List<?> list(Object o) { return (List<?>) o; }
    static String str(Object o) { return (String) o; }
    static Map<String, Object> map(Object... pairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) m.put((String) pairs[i], pairs[i + 1]); return m;
    }
    static Map<?, ?> node(int id) {
        return list(cluster.get("nodes")).stream().map(OutcomeJdbc::obj)
                .filter(n -> ((Number) n.get("id")).intValue() == id).findFirst().orElseThrow();
    }
    static String host(Object h) { String s = str(h); return s.contains(":") && !s.startsWith("[") ? "[" + s + "]" : s; }
    static Map<?, ?> agent(int id, String path, Object body) throws Exception {
        Map<?, ?> n = node(id);
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://" + host(n.get("host")) + ":" + n.get("agent_port") + path))
                .timeout(Duration.ofSeconds(130)).header("Content-Type", "application/json");
        if (body == null) b.GET(); else b.POST(HttpRequest.BodyPublishers.ofString(Json.write(body)));
        HttpResponse<String> response = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) throw new IllegalStateException(path + ": HTTP " + response.statusCode() + ": " + response.body());
        return obj(Json.read(response.body()));
    }
    static Connection connect(int id) throws SQLException {
        Map<?, ?> n = node(id); Properties p = new Properties();
        p.setProperty("user", str(cluster.get("owner"))); p.setProperty("password", str(cluster.get("password")));
        p.setProperty("preferQueryMode", "simple"); p.setProperty("connectTimeout", "2");
        p.setProperty("socketTimeout", "50"); p.setProperty("sslmode", "disable");
        Connection c = DriverManager.getConnection("jdbc:postgresql://" + host(n.get("host")) + ":" + n.get("pg_port") + "/" + cluster.get("database"), p);
        try { c.setAutoCommit(true); execute(c, "SET statement_timeout = 45000"); return c; }
        catch (SQLException e) { c.close(); throw e; }
    }
    static void execute(Connection c, String sql) throws SQLException { try (Statement s = c.createStatement()) { s.execute(sql); } }
    static String scalar(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) { if (!r.next()) throw new SQLException("no row"); return r.getString(1); }
    }
    static double restart(int gateway) throws Exception {
        double start = now(); agent(gateway, "/start", map("mode", cluster.get("mode_default")));
        double deadline = now() + 60;
        while (!Boolean.TRUE.equals(agent(gateway, "/status", null).get("ready"))) {
            if (now() >= deadline) throw new TimeoutException("gateway agent not ready"); Thread.sleep(100);
        }
        deadline = now() + 60; SQLException last = null;
        while (now() < deadline) {
            try (Connection c = connect(gateway)) { if (scalar(c, "SELECT 1").equals("1")) return now() - start; }
            catch (SQLException e) { last = e; } Thread.sleep(100);
        }
        throw new TimeoutException("gateway SQL not ready: " + last);
    }
    static Map<String, Object> commit(Connection c, CountDownLatch sent) {
        sent.countDown();
        try { execute(c, "COMMIT"); return map("view", "success", "message", "COMMIT"); }
        catch (SQLException e) { return map("view", "error", "sqlstate", e.getSQLState(), "connection_error",
                e.getSQLState() != null && e.getSQLState().startsWith("08"), "error", e.toString()); }
    }
    static List<Object> rows(Connection c, String table, int number, boolean text) throws SQLException {
        List<Object> rows = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT * FROM " + table + " WHERE id = " + number)) {
            while (r.next()) rows.add(List.of(r.getLong(1), text ? r.getString(2) : r.getLong(2)));
        }
        return rows;
    }
    static Map<String, Object> trial(String a, String b, int number, int gateway) throws Exception {
        double begin = now(); List<String> violations = new ArrayList<>(); Map<String, Object> timings = map();
        Map<String, Object> row = map("trial", number, "point", "during", "gateway", gateway, "observer", 1,
                "violations", violations, "timings", timings, "restart_before_resolution", false);
        boolean killed = false, restarted = false; Connection observed = null;
        try (Connection c = connect(gateway)) {
            execute(c, "BEGIN"); execute(c, "INSERT INTO " + a + " VALUES (" + number + ", 'x')");
            execute(c, "INSERT INTO " + b + " VALUES (" + number + ", 1)");
            long xid = Long.parseLong(scalar(c, "SELECT pg_current_xact_id()::text")); row.put("xid", xid);
            CountDownLatch sent = new CountDownLatch(1); ExecutorService pool = Executors.newSingleThreadExecutor();
            Map<String, Object> client;
            try {
                Future<Map<String, Object>> future = pool.submit(() -> commit(c, sent));
                if (!sent.await(5, TimeUnit.SECONDS)) throw new TimeoutException("commit thread did not start");
                double delay = ThreadLocalRandom.current().nextDouble(30); row.put("kill_delay_ms", delay);
                TimeUnit.NANOSECONDS.sleep((long) (delay * 1e6)); killed = true; agent(gateway, "/kill", map());
                client = future.get(10, TimeUnit.SECONDS);
            } finally { pool.shutdownNow(); }
            row.put("client", client); timings.put("kill_seconds", now() - begin);
            double deadline = now() + 60, pollBegin = now(); String status = null, last = null;
            while (now() < deadline) {
                try {
                    if (observed == null || observed.isClosed()) observed = connect(1);
                    execute(observed, "SET statement_timeout = " + Math.max(1, (int) (Math.min(45, deadline - now()) * 1000)));
                    status = scalar(observed, "SELECT pg_xact_status(" + xid + ")");
                    if ("committed".equals(status) || "aborted".equals(status)) break;
                    if (!"in progress".equals(status)) last = "unexpected status " + status;
                } catch (SQLException e) { last = e.toString(); if (observed != null) observed.close(); observed = null; }
                if ("in progress".equals(status) && !restarted && now() - pollBegin >= 10) {
                    row.put("restart_before_resolution", true); timings.put("restart_seconds", restart(gateway)); restarted = true;
                }
                Thread.sleep(200);
            }
            row.put("status", status); row.put("last_poll_error", last); timings.put("resolution_seconds", now() - pollBegin);
            if (!("committed".equals(status) || "aborted".equals(status)) || now() > deadline) violations.add("no definite outcome within 60 seconds");
            if (observed == null || observed.isClosed()) observed = connect(1);
            List<Object> actual = List.of(rows(observed, a, number, true), rows(observed, b, number, false)); row.put("rows", actual);
            List<?> expected = "committed".equals(status) ? List.of(List.of(List.of((long) number, "x")), List.of(List.of((long) number, 1L))) : List.of(List.of(), List.of());
            if (!actual.equals(expected)) violations.add("rows disagree with outcome or are partial");
            if (client.get("view").equals("success") && !"committed".equals(status)) violations.add("acknowledged commit not committed");
        } finally {
            if (observed != null) observed.close();
            if (killed && !restarted) timings.put("restart_seconds", restart(gateway)); timings.put("total_seconds", now() - begin);
        }
        return row;
    }
    static void save(Path out, Map<String, Object> result) throws Exception {
        Files.writeString(out.resolve("outcome-jdbc.json"), Json.write(result) + "\n");
    }
    public static void main(String[] argv) throws Exception {
        Map<String, String> args = new LinkedHashMap<>();
        for (int i = 0; i < argv.length; i++) {
            if (!Set.of("--cluster", "--out", "--trials").contains(argv[i])) throw new IllegalArgumentException(argv[i]);
            args.put(argv[i], argv[++i]);
        }
        int count = Integer.parseInt(args.getOrDefault("--trials", "10"));
        if (!args.containsKey("--cluster") || !args.containsKey("--out") || count < 1) throw new IllegalArgumentException("--cluster FILE --out DIR [--trials N>0] required");
        Path out = Path.of(args.get("--out")); Files.createDirectories(out); List<Object> trials = new ArrayList<>();
        Map<String, Object> result = map("trials", trials, "counts", map()); int rc;
        try {
            cluster = obj(Json.read(Files.readString(Path.of(args.get("--cluster")))));
            if (((Number) cluster.get("version")).intValue() != 1) throw new IllegalArgumentException("unsupported cluster.json version");
            List<Integer> gateways = list(cluster.get("nodes")).stream().map(OutcomeJdbc::obj)
                    .map(n -> ((Number) n.get("id")).intValue()).filter(n -> n != 1).toList();
            String id = UUID.randomUUID().toString().replace("-", "").substring(0, 12), a = "oa_" + id, b = "ob_" + id;
            result.put("tables", List.of(a, b)); result.put("mode", cluster.get("mode_default"));
            try (Connection c = connect(1)) {
                execute(c, "CREATE TABLE " + a + " (id bigint primary key, note text)"); execute(c, "CREATE TABLE " + b + " (id bigint primary key, n bigint)");
            }
            int failed = 0;
            for (int i = 1; i <= count; i++) {
                Map<String, Object> row = trial(a, b, i, gateways.get((i - 1) % gateways.size())); trials.add(row);
                if (!list(row.get("violations")).isEmpty()) failed++; save(out, result);
            }
            Map<String, Object> counts = map(); if (count > failed) counts.put("pass", count - failed); if (failed > 0) counts.put("fail", failed);
            result.put("counts", map("during", counts)); rc = failed > 0 ? 1 : 0;
        } catch (Exception e) { result.put("infrastructure_error", e.toString()); rc = 2; }
        result.put("exit_code", rc); save(out, result);
        String summary = "outcome-jdbc: " + (rc == 0 ? "PASS" : rc == 1 ? "VIOLATION" : "INFRASTRUCTURE") + "; trials=" + trials.size() + " counts=" + Json.write(result.get("counts"));
        Files.writeString(out.resolve("summary.md"), summary + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND); System.out.println(summary); System.exit(rc);
    }
    static class Json {
        final String s; int p;
        Json(String text) { s = text; }
        static Object read(String s) { Json j = new Json(s); Object o = j.value(); j.ws(); if (j.p != s.length()) throw new IllegalArgumentException("trailing JSON"); return o; }
        void ws() { while (p < s.length() && Character.isWhitespace(s.charAt(p))) p++; }
        char take() { return s.charAt(p++); }
        Object value() {
            ws(); char c = take();
            if (c == '"') { StringBuilder b = new StringBuilder(); while ((c = take()) != '"') {
                if (c == '\\') { c = take(); if (c == 'u') { b.append((char) Integer.parseInt(s.substring(p, p + 4), 16)); p += 4; continue; }
                    c = switch (c) { case 'n' -> '\n'; case 'r' -> '\r'; case 't' -> '\t'; case 'b' -> '\b'; case 'f' -> '\f'; default -> c; }; }
                b.append(c); } return b.toString(); }
            if (c == '{' || c == '[') {
                Map<String, Object> m = new LinkedHashMap<>(); List<Object> a = new ArrayList<>(); ws(); char end = c == '{' ? '}' : ']';
                if (s.charAt(p) != end) while (true) {
                    if (c == '{') { String k = str(value()); ws(); if (take() != ':') throw new IllegalArgumentException("JSON colon"); m.put(k, value()); }
                    else a.add(value()); ws(); if (s.charAt(p) != ',') break; p++;
                }
                if (take() != end) throw new IllegalArgumentException("JSON end"); return c == '{' ? m : a;
            }
            int begin = p - 1; while (p < s.length() && ",]} \r\n\t".indexOf(s.charAt(p)) < 0) p++;
            String v = s.substring(begin, p);
            if (v.equals("null")) return null; if (v.equals("true")) return true; if (v.equals("false")) return false;
            if (v.matches("-?\\d+")) return Long.valueOf(v); return Double.valueOf(v);
        }
        static String write(Object o) {
            if (o == null) return "null";
            if (o instanceof String s) { StringBuilder b = new StringBuilder("\""); for (char c : s.toCharArray()) {
                if (c == '"' || c == '\\') b.append('\\').append(c); else if (c < 32) b.append(String.format("\\u%04x", (int) c)); else b.append(c);
            } return b.append('"').toString(); }
            List<String> parts = new ArrayList<>();
            if (o instanceof Map<?, ?> m) { m.forEach((k, v) -> parts.add(write(k) + ":" + write(v))); return "{" + String.join(",", parts) + "}"; }
            if (o instanceof Iterable<?> a) { a.forEach(v -> parts.add(write(v))); return "[" + String.join(",", parts) + "]"; }
            if (o instanceof boolean[] a) { for (boolean b : a) parts.add(write(b)); return "[" + String.join(",", parts) + "]"; }
            return o.toString();
        }
    }
}
