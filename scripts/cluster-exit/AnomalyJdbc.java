import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/** PostgreSQL simple-query mirror of anomaly.py; no dependencies besides pgJDBC. */
public class AnomalyJdbc {
    static Map<String, String> args;
    static Map<?, ?> data, cluster;
    static List<Integer> nodes;
    static String runId;
    static boolean strict, keep;
    static double now() { return System.nanoTime() / 1e9; }
    static Map<String, Object> map(Object... pairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) m.put((String) pairs[i], pairs[i + 1]);
        return m;
    }
    static Map<?, ?> obj(Object o) { return (Map<?, ?>) o; }
    static List<?> list(Object o) { return (List<?>) o; }
    static String str(Object o) { return (String) o; }
    static Map<String, String> options(String[] argv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String k = argv[i];
            if (!Set.of("--cluster", "--target", "--url", "--out", "--cases", "--forms", "--nodes",
                    "--run-id", "--strict-pg", "--keep-tables").contains(k)) throw new IllegalArgumentException(k);
            m.put(k, Set.of("--strict-pg", "--keep-tables").contains(k) ? "true" : argv[++i]);
        }
        if (!m.containsKey("--out") || m.containsKey("--cluster") == m.containsKey("--target")
                || (m.containsKey("--target") && (!"pg".equals(m.get("--target")) || !m.containsKey("--url"))))
            throw new IllegalArgumentException("--out and (--cluster FILE | --target pg --url URL) required");
        return m;
    }
    static String host(String h) { return h.contains(":") && !h.startsWith("[") ? "[" + h + "]" : h; }
    static Connection connect(int index) throws SQLException {
        Properties p = new Properties();
        p.setProperty("preferQueryMode", "simple"); p.setProperty("connectTimeout", "10");
        p.setProperty("socketTimeout", "65"); p.setProperty("sslmode", "disable");
        String url = args.get("--url");
        if (cluster != null) {
            Map<?, ?> n = list(cluster.get("nodes")).stream().map(AnomalyJdbc::obj)
                    .filter(x -> ((Number) x.get("id")).intValue() == nodes.get(index)).findFirst().orElseThrow();
            url = "jdbc:postgresql://" + host(str(n.get("host"))) + ":" + n.get("pg_port") + "/" + cluster.get("database");
            p.setProperty("user", str(cluster.get("owner"))); p.setProperty("password", str(cluster.get("password")));
        }
        Connection c = DriverManager.getConnection(url, p); c.setAutoCommit(true); return c;
    }
    record Query(List<String> rows, long count) {}
    static Query simple(Connection c, String sql, Session session) throws SQLException {
        List<String> rows = new ArrayList<>(); long count = 0; boolean first = true;
        try (Statement st = c.createStatement()) {
            if (session != null) session.active = st;
            boolean result = st.execute(sql);
            while (true) {
                long update = st.getLargeUpdateCount();
                if (!result && update == -1) break;
                if (result) try (ResultSet rs = st.getResultSet()) {
                    int columns = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        List<String> row = new ArrayList<>();
                        for (int col = 1; col <= columns; col++) row.add(Objects.toString(rs.getString(col), ""));
                        rows.add(String.join("|", row));
                    }
                }
                if (first) { count = result ? rows.size() : update; first = false; }
                result = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
            }
        } finally { if (session != null) session.active = null; }
        return new Query(rows, count);
    }
    static class Session {
        final Connection conn; final String form; boolean block, failed; volatile Statement active;
        Session(Connection c, String f) { conn = c; form = f; }
        String step(String sql) throws SQLException {
            String verb = sql.strip().split("\\s+")[0]; List<String> queries = List.of(sql);
            if (sql.startsWith("BEGIN ISOLATION LEVEL ") && !form.equals("begin")) {
                String rest = sql.substring(22), level = rest.replaceFirst(" READ ONLY$", "");
                String tx = "SET TRANSACTION ISOLATION LEVEL " + rest;
                queries = switch (form) {
                    case "session" -> List.of("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + level,
                            rest.endsWith(" READ ONLY") ? "BEGIN READ ONLY" : "BEGIN");
                    case "set_transaction" -> List.of("BEGIN", tx);
                    default -> List.of("BEGIN; " + tx);
                };
            }
            boolean ok = true; String out;
            try {
                Query q = null; for (String query : queries) q = simple(conn, query, this);
                out = switch (verb) {
                    case "SELECT" -> String.join(",", Objects.requireNonNull(q).rows());
                    case "BEGIN" -> "BEGIN";
                    case "COMMIT" -> failed ? "ROLLBACK" : "COMMIT";
                    case "INSERT" -> "INSERT 0 " + Objects.requireNonNull(q).count();
                    default -> verb + " " + Objects.requireNonNull(q).count();
                };
            } catch (SQLException e) {
                String state = e.getSQLState();
                if (state == null || state.startsWith("08") || Set.of("57P01", "57P02", "57P03").contains(state)) throw e;
                ok = false; out = "E:" + state;
            }
            if (verb.equals("BEGIN")) { block = ok; failed = false; }
            else if (verb.equals("COMMIT") || verb.equals("ROLLBACK")) { block = false; failed = false; }
            else failed |= block && !ok;
            return out;
        }
    }
    static String suffix(String sql, List<String> tables, String tail) {
        StringBuilder out = new StringBuilder(), word = new StringBuilder(); boolean quoted = false;
        for (char ch : (sql + " ").toCharArray()) {
            if (!quoted && ch < 128 && (Character.isLetterOrDigit(ch) || ch == '_')) { word.append(ch); continue; }
            out.append(word); if (tables.contains(word.toString())) out.append(tail); word.setLength(0);
            if (ch == '\'') quoted = !quoted; out.append(ch);
        }
        return out.substring(0, out.length() - 1);
    }
    static List<String> tables(String setup) {
        return Arrays.stream(setup.split("CREATE TABLE ")).skip(1).map(s -> s.split("\\s+")[0]).toList();
    }
    static void drop(Connection c, List<String> tables, String tail) throws SQLException {
        if (!keep) for (int i = tables.size() - 1; i >= 0; i--)
            simple(c, "DROP TABLE IF EXISTS " + tables.get(i) + tail + " CASCADE", null);
    }
    static Map<String, Object> record(String name, Object level, Object form) {
        return map("case", name, "level", level, "form", form, "transcript", new ArrayList<String>(), "mismatches", new ArrayList<String>());
    }
    static void compare(Map<String, Object> r, String label, String spec, String ours, int level, boolean exact) {
        String[] parts = spec.split(";"), choices = parts[parts.length == 3 ? level : 0].split(" or ");
        List<String> admitted = exact ? List.of(choices[0]) : Arrays.asList(choices);
        add(r, "transcript", label + ": pg " + Json.write(choices[0]) + " ours " + Json.write(ours));
        if (!admitted.contains(ours)) add(r, "mismatches", label + ": admitted " + Json.write(admitted) + ", got " + Json.write(ours));
    }
    static void add(Map<String, Object> r, String key, String value) {
        Object old = r.get(key); List<Object> copy = new ArrayList<>(list(old)); copy.add(value); r.put(key, copy);
    }
    record Step(int index, String sql) {}
    record Done(int index, String out, double duration, Exception error) {}
    static Map<String, Object> runCase(Map<?, ?> c, int level, String form) throws Exception {
        double start = now(); String name = str(c.get("name")), isolation = str(list(data.get("levels")).get(level));
        Map<String, Object> r = record(name, isolation, form); List<?> steps = list(c.get("steps"));
        List<String> tables = tables(str(c.get("setup")));
        String tail = "_" + runId + "_" + name + "_" + List.of("rc", "rr", "ser").get(level) + "_" + list(data.get("forms")).indexOf(form);
        java.util.function.UnaryOperator<String> rewrite = sql -> suffix(sql.replace("@LEVEL@", isolation), tables, tail);
        try (Connection conn = connect(0)) { simple(conn, rewrite.apply(str(c.get("setup"))), null); }
        BlockingQueue<Done> done = new LinkedBlockingQueue<>(); List<BlockingQueue<Step>> inputs = new ArrayList<>();
        List<Session> sessions = new ArrayList<>(); List<Thread> threads = new ArrayList<>();
        Done[] outcomes = new Done[steps.size()]; boolean[] waited = new boolean[steps.size()];
        try {
            for (int node = 0; node < 3; node++) {
                Session session = new Session(connect(node), form); sessions.add(session);
                BlockingQueue<Step> inbox = new LinkedBlockingQueue<>(); inputs.add(inbox);
                Thread thread = new Thread(() -> {
                    try { while (true) {
                        Step s = inbox.take(); if (s.index() < 0) return; double begin = now();
                        try { done.add(new Done(s.index(), session.step(s.sql()), now() - begin, null)); }
                        catch (Exception e) { done.add(new Done(s.index(), null, now() - begin, e)); return; }
                    } } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
                thread.setDaemon(true); threads.add(thread); thread.start();
            }
            for (int i = 0; i < steps.size(); i++) {
                List<?> step = list(steps.get(i)); inputs.get("ABC".indexOf(str(step.get(0)))).add(new Step(i, rewrite.apply(str(step.get(1)))));
                double deadline = now() + ((Number) data.get("step_wait_s")).doubleValue();
                while (outcomes[i] == null) if (!receive(done, outcomes, deadline)) { waited[i] = true; break; }
            }
            for (BlockingQueue<Step> inbox : inputs) inbox.add(new Step(-1, ""));
            double deadline = now() + ((Number) data.get("end_wait_s")).doubleValue();
            while (Arrays.stream(outcomes).anyMatch(Objects::isNull) && receive(done, outcomes, deadline)) { /* drain */ }
        } finally {
            for (BlockingQueue<Step> inbox : inputs) inbox.add(new Step(-1, ""));
            for (int i = 0; i < sessions.size(); i++) {
                Session s = sessions.get(i); Thread t = i < threads.size() ? threads.get(i) : null;
                Statement active = s.active; if (t != null && t.isAlive() && active != null) active.cancel();
                if (t != null) t.join(5000); s.conn.close();
                if (t != null && t.isAlive()) throw new IllegalStateException("session worker did not stop after cancellation");
            }
        }
        for (int i = 0; i < steps.size(); i++) {
            List<?> s = list(steps.get(i)); String out = (waited[i] ? "~" : "") + (outcomes[i] == null ? "TIMEOUT" : outcomes[i].out());
            compare(r, "[" + (i + 1) + "] " + s.get(0) + ": " + s.get(1), str(s.get(2)), out, level, strict);
        }
        List<?> fin = list(c.get("fin")); String out;
        try (Connection conn = connect(0)) {
            try { out = new Session(conn, form).step(rewrite.apply(str(fin.get(0)))); }
            finally { drop(conn, tables, tail); }
        }
        compare(r, "final: " + fin.get(0), str(fin.get(1)), out, level, strict);
        r.put("seconds", now() - start); r.put("waited", waited);
        r.put("step_seconds", Arrays.stream(outcomes).map(d -> d == null ? null : d.duration()).toList()); return r;
    }
    static boolean receive(BlockingQueue<Done> queue, Done[] out, double deadline) throws Exception {
        Done d = queue.poll(Math.max(0, (long) ((deadline - now()) * 1e9)), TimeUnit.NANOSECONDS);
        if (d == null) return false; if (d.error() != null) throw d.error(); out[d.index()] = d; return true;
    }
    static void showCheck(Connection c, Map<String, Object> r, String sql, String expected, String context) throws SQLException {
        List<String> rows = simple(c, sql, null).rows(); compare(r, context + ": " + sql, expected, rows.isEmpty() ? "" : rows.get(0), 0, true);
    }
    static Map<String, Object> reports() throws SQLException {
        double start = now(); Map<String, Object> r = record("reports_each_level", null, null);
        String current = "SHOW transaction_isolation", def = "SHOW default_transaction_isolation";
        try (Connection c = connect(1)) {
            showCheck(c, r, current, "read committed", "initial");
            for (Object value : list(data.get("levels"))) {
                String level = str(value);
                for (String begin : List.of("BEGIN ISOLATION LEVEL " + level, "BEGIN; SET TRANSACTION ISOLATION LEVEL " + level)) {
                    simple(c, begin, null); showCheck(c, r, current, level.toLowerCase(Locale.ROOT), begin);
                    showCheck(c, r, def, "read committed", begin); simple(c, "ROLLBACK", null);
                    showCheck(c, r, current, "read committed", begin + "; ROLLBACK");
                }
                String statement = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + level;
                simple(c, statement, null); showCheck(c, r, current, level.toLowerCase(Locale.ROOT), statement);
                showCheck(c, r, def, level.toLowerCase(Locale.ROOT), statement); simple(c, "BEGIN", null);
                showCheck(c, r, current, level.toLowerCase(Locale.ROOT), statement + "; BEGIN");
                simple(c, "COMMIT", null); simple(c, "RESET default_transaction_isolation", null);
            }
        }
        r.put("seconds", now() - start); return r;
    }
    static Map<String, Object> union() throws SQLException {
        double start = now(); Map<String, Object> r = record("foreign_key_union_all", null, "begin");
        Map<?, ?> f = obj(data.get("union_finding")); String tail = "_" + runId + "_foreign_key_union_all"; String out;
        try (Connection c = connect(0)) {
            simple(c, suffix(str(f.get("setup")), List.of("p", "c"), tail), null);
            try { out = new Session(c, "begin").step(suffix(str(f.get("sql")), List.of("p", "c"), tail)); }
            finally { drop(c, List.of("p", "c"), tail); }
        }
        compare(r, str(f.get("sql")), str(f.get("spec")), out, 0, strict); r.put("seconds", now() - start); return r;
    }
    static int mismatches(List<?> results) {
        return results.stream().map(AnomalyJdbc::obj).mapToInt(r -> list(r.get("mismatches")).size()).sum();
    }
    public static void main(String[] argv) throws Exception {
        args = options(argv); Path source = Path.of(System.getProperty("jdk.launcher.sourcefile", "scripts/cluster-exit/AnomalyJdbc.java"));
        data = obj(Json.read(Files.readString(source.toAbsolutePath().getParent().resolve("anomaly_cases.json"))));
        nodes = Arrays.stream(args.getOrDefault("--nodes", "1,2,3").split(",")).map(Integer::valueOf).toList();
        if (nodes.size() != 3 || new HashSet<>(nodes).size() != 3) throw new IllegalArgumentException("three distinct node ids required");
        runId = args.getOrDefault("--run-id", UUID.randomUUID().toString().substring(0, 6));
        if (!runId.matches("[a-zA-Z0-9_]{1,16}")) throw new IllegalArgumentException("invalid run id");
        strict = args.containsKey("--strict-pg"); keep = args.containsKey("--keep-tables");
        List<String> names = list(data.get("cases")).stream().map(AnomalyJdbc::obj).map(c -> str(c.get("name"))).toList();
        List<String> cases = args.containsKey("--cases") ? Arrays.asList(args.get("--cases").split(",")) : names;
        List<?> forms = args.containsKey("--forms") ? Arrays.asList(args.get("--forms").split(",")) : list(data.get("forms"));
        if (!names.containsAll(cases) || !list(data.get("forms")).containsAll(forms) || new HashSet<>(forms).size() != forms.size())
            throw new IllegalArgumentException("unknown cases or unknown/duplicate forms");
        List<Object> runs = new ArrayList<>(), findings = new ArrayList<>(); List<String> infra = new ArrayList<>();
        Map<String, Object> output = map("source", data.get("source"), "run_id", runId, "strict_pg", strict,
                "target", args.getOrDefault("--target", "cluster"), "nodes", nodes, "pgjdbc", "42.7.3",
                "runs", runs, "findings", findings, "reports_each_level", null, "infrastructure", infra);
        double start = now();
        try {
            if (args.containsKey("--cluster")) {
                cluster = obj(Json.read(Files.readString(Path.of(args.get("--cluster")))));
                if (((Number) cluster.get("version")).intValue() != 1) throw new IllegalArgumentException("unsupported cluster.json version");
            }
            output.put("reports_each_level", reports());
            for (Object form : forms) for (Object value : list(data.get("cases"))) {
                Map<?, ?> c = obj(value); String name = str(c.get("name"));
                if (!cases.contains(name) || (!form.equals("begin") && !list(data.get("again")).contains(name))) continue;
                for (int level = 0; level < list(data.get("levels")).size(); level++) {
                    Map<String, Object> r = runCase(c, level, str(form)); boolean finding = false;
                    for (Object pair : list(data.get("findings"))) {
                        List<?> p = list(pair); finding |= p.get(0).equals(name) && ((Number) p.get(1)).intValue() == level;
                    }
                    (finding ? findings : runs).add(r);
                    System.out.println(name + " / " + r.get("level") + " / " + form + ": " + list(r.get("mismatches")).size() + " mismatch(es)");
                }
            }
            if (forms.contains("begin") && list(data.get("findings")).stream().anyMatch(p -> cases.contains(list(p).get(0)))) findings.add(union());
        } catch (Exception e) { infra.add(e.getClass().getSimpleName() + ": " + e.getMessage()); }
        int mismatch = mismatches(runs) + (output.get("reports_each_level") == null ? 0 : mismatches(List.of(output.get("reports_each_level"))));
        int rc = !infra.isEmpty() ? 2 : mismatch == 0 ? 0 : 1;
        output.put("seconds", now() - start); output.put("pass", rc == 0); output.put("exit_code", rc);
        Path out = Path.of(args.get("--out")); Files.createDirectories(out); Files.writeString(out.resolve("anomaly-jdbc.json"), Json.write(output) + "\n");
        String summary = "anomaly-jdbc " + runId + ": " + (rc == 0 ? "PASS" : "FAIL") + "; " + runs.size()
                + " matrix runs, " + mismatch + " mismatches; " + findings.size() + " findings, " + mismatches(findings)
                + " finding mismatches; " + infra.size() + " infrastructure errors; " + (now() - start) + "s";
        Files.writeString(out.resolve("summary.md"), summary + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        System.out.println(summary); System.exit(rc);
    }
    // Minimal JSON codec, shared verbatim with the independently launchable outcome source.
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
