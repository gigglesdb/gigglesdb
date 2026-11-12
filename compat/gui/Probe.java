import java.sql.*;
import java.util.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;

/** Bounded source-derived DBeaver queries plus pgJDBC metadata calls. */
public class Probe {
  static String quote(String s) { return "\""+s.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n").replace("\r","\\r").replace("\t","\\t")+"\""; }
  static String json(Object v) {
    if(v==null)return "null";
    if(v instanceof Map<?,?> m)return "{"+String.join(",",m.entrySet().stream().map(e->quote(e.getKey().toString())+":"+json(e.getValue())).toList())+"}";
    if(v instanceof List<?> l)return "["+String.join(",",l.stream().map(Probe::json).toList())+"]";
    if(v instanceof Number || v instanceof Boolean)return v.toString();
    return quote(v.toString());
  }
  static Map<String,Object> result(ResultSet r) throws SQLException {
    var m=r.getMetaData();var columns=new ArrayList<Object>();var rows=new ArrayList<Object>();
    for(int i=1;i<=m.getColumnCount();i++)columns.add(List.of(m.getColumnLabel(i),m.getColumnType(i)));
    while(r.next()) {var row=new ArrayList<Object>();for(int i=1;i<=m.getColumnCount();i++)row.add(r.getString(i));rows.add(row);if(rows.size()>10000)throw new SQLException("row cap exceeded");}
    return Map.of("columns",columns,"rows",rows,"row_count",rows.size());
  }
  public static void main(String[] args) throws Exception {
    Properties p=new Properties();p.setProperty("user","postgres");p.setProperty("password",System.getenv("PGPASSWORD"));p.setProperty("ApplicationName","p1-gui");p.setProperty("socketTimeout","30");
    try(Connection c=DriverManager.getConnection(args[0],p)) {
      for(String line:Files.readAllLines(Path.of(args[1]),StandardCharsets.UTF_8)) {
        String[] a=line.split("\t",-1);String sql=new String(Base64.getDecoder().decode(a[1]),StandardCharsets.UTF_8);
        Map<String,Object> out=new LinkedHashMap<>();out.put("id",a[0]);out.put("sql",sql);
        try {
          c.setClientInfo("ApplicationName","p1-gui:"+a[0]);
          if(sql.startsWith("META:")) {
            DatabaseMetaData d=c.getMetaData();
            try(ResultSet r=switch(sql.substring(5)) {
              case "schemas" -> d.getSchemas(null,"gui_%");
              case "tables" -> d.getTables(null,"gui_sales","%",new String[]{"TABLE","VIEW"});
              case "columns" -> d.getColumns(null,"gui_sales","child","%");
              case "indexes" -> d.getIndexInfo(null,"gui_sales","child",false,false);
              case "pk" -> d.getPrimaryKeys(null,"gui_sales","child");
              case "fk" -> d.getImportedKeys(null,"gui_sales","child");
              default -> throw new SQLException("unknown metadata method");
            }) {out.putAll(result(r));}
          } else {
            try(PreparedStatement s=c.prepareStatement(sql)) {
              s.setQueryTimeout(30);
              if(a.length>2 && !a[2].isEmpty()) {
                String lookup=a[2].equals("schema")?"SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='gui_sales'":"SELECT oid FROM pg_catalog.pg_class WHERE relname='child' AND relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='gui_sales')";
                try(var q=c.createStatement();var r=q.executeQuery(lookup)) {if(!r.next())throw new SQLException("missing sample object", "P0002");long oid=r.getLong(1);s.setLong(1,oid);out.put("parameters",List.of(oid));}
              }
              if(s.execute()) {try(ResultSet r=s.getResultSet()){out.putAll(result(r));}}else out.put("update_count",s.getUpdateCount());
              var more=new ArrayList<Object>();
              while(s.getMoreResults() || s.getUpdateCount()!=-1) {
                if(s.getResultSet()!=null) {try(var r=s.getResultSet()){more.add(result(r));}} else more.add(Map.of("update_count",s.getUpdateCount()));
              }
              if(!more.isEmpty())out.put("additional_results",more);
            }
          }
          out.put("status","OK");
        } catch(SQLException e) {out.put("status","ERR");out.put("sqlstate",e.getSQLState());out.put("error",e.getMessage());}
        System.out.println(json(out));
      }
    }
  }
}
