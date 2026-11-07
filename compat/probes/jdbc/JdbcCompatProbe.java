import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

/** Reuses GD0048's asserting JDBC probe at crates/giggles-pg-conformance/probes/JdbcProbe.java
 * and strict record discipline at crates/giggles-pg-conformance/src/lib.rs,
 * without its mTLS fixture assumptions. */
public final class JdbcCompatProbe {
  static String table;
  interface Check { Object run(Connection c) throws Exception; }
  static final class ProbeAssertion extends AssertionError {
    final String reason;
    ProbeAssertion(String reason) { super(reason); this.reason=reason; }
  }
  static void require(boolean value,String reason) { if (!value) throw new ProbeAssertion(reason); }
  static Map<String,Object> obj(Object... pairs) {
    Map<String,Object> result = new TreeMap<>();
    for (int i=0;i<pairs.length;i+=2) result.put((String)pairs[i],pairs[i+1]);
    return result;
  }
  static String env(String name,String fallback) { return System.getenv().getOrDefault(name,fallback); }
  static Connection connect() throws SQLException {
    Properties p=new Properties();
    p.setProperty("ApplicationName",env("PGAPPNAME",""));
    p.setProperty("user",env("PGUSER","postgres"));p.setProperty("password",env("PGPASSWORD",""));
    p.setProperty("sslmode","disable");p.setProperty("connectTimeout","10");
    p.setProperty("socketTimeout","30");p.setProperty("prepareThreshold","5");
    String host=env("PGHOST","localhost");
    if(host.contains(":" )&&!host.startsWith("["))host="["+host+"]";
    // Encode the database path independently; credentials always remain connection properties.
    String db=java.net.URLEncoder.encode(env("PGDATABASE","postgres"),StandardCharsets.UTF_8).replace("+","%20");
    return DriverManager.getConnection("jdbc:postgresql://"+host+":"+env("PGPORT","5432")+"/"+db,p);
  }
  interface Step { Object run() throws Exception; }
  static Map<String,Object> observe(Step step) {
    try {
      Object data = step.run();
      json(data); // Serialization failure belongs to this observation as well.
      return obj("status", "OK", "data", data);
    } catch (Throwable e) {
      Map<String,Object> result=error(e);result.put("status","ERR");return result;
    }
  }
  static boolean ok(Map<String,Object> observation) { return "OK".equals(observation.get("status")); }
  static Object canonical(Object value) {
    if (value instanceof byte[] bytes) return HexFormat.of().formatHex(bytes);
    if (value instanceof BigDecimal decimal) return decimal.toPlainString();
    if (value instanceof Timestamp timestamp) return timestamp.toInstant().toString();
    if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant().toString();
    if (value instanceof UUID uuid) return uuid.toString();
    if (value instanceof Short number) return number.intValue();
    return value;
  }
  static Map<String,Object> error(Throwable e) {
    Map<String,Object> result=obj("sqlstate",e instanceof SQLException ? ((SQLException)e).getSQLState():null,"exception_class",e.getClass().getName());
    if(e instanceof ProbeAssertion assertion)result.put("reason",assertion.reason);
    return result;
  }
  static void test(String id,Check check) {
    String encoded;String status;
    try(Connection c=connect()){encoded=Base64.getEncoder().encodeToString(json(check.run(c)).getBytes(StandardCharsets.UTF_8));status="OK";}
    catch(Throwable e){encoded=Base64.getEncoder().encodeToString(json(error(e)).getBytes(StandardCharsets.UTF_8));status="ERR";}
    System.out.println("CASE\t"+id+"\t"+status+"\t"+encoded);
  }
  static String json(Object value) {
    if(value==null)return "null";
    if(value instanceof String s){StringBuilder b=new StringBuilder("\"");for(int i=0;i<s.length();i++){
      char ch=s.charAt(i);switch(ch){case '"':b.append("\\\"");break;case '\\':b.append("\\\\");break;
        case '\b':b.append("\\b");break;case '\f':b.append("\\f");break;case '\n':b.append("\\n");break;
        case '\r':b.append("\\r");break;case '\t':b.append("\\t");break;
        default:if(ch<32||Character.isSurrogate(ch))b.append(String.format(Locale.ROOT,"\\u%04x",(int)ch));else b.append(ch);}}
      return b.append('"').toString();}
    if(value instanceof BigDecimal n)return json(n.toPlainString());
    if(value instanceof Number||value instanceof Boolean)return value.toString();
    if(value instanceof Map<?,?> m){List<String> fields=new ArrayList<>();for(var e:m.entrySet())fields.add(json(e.getKey())+":"+json(e.getValue()));return "{"+String.join(",",fields)+"}";}
    if(value instanceof Iterable<?> list){List<String> fields=new ArrayList<>();for(Object x:list)fields.add(json(x));return "["+String.join(",",fields)+"]";}
    throw new ProbeAssertion("unsupported_observation_type");
  }
  static Object scalar(Connection c,String sql,boolean prepared) throws SQLException {
    try(Statement s=prepared?c.prepareStatement(sql):c.createStatement();ResultSet r=prepared?((PreparedStatement)s).executeQuery():s.executeQuery(sql)){
      require(r.next(),"missing_row");Object value=r.getObject(1);int type=r.getMetaData().getColumnType(1);require(!r.next(),"unexpected_row_count");
      return obj("value",value,"jdbc_type",type);
    }
  }
  static int insert(Connection c,int id) throws SQLException {
    try(var s=c.prepareStatement("INSERT INTO "+table+" (id,name) VALUES (?,?)")){s.setInt(1,id);s.setString(2,"tx"+id);int n=s.executeUpdate();require(n==1,"update_count_mismatch");return n;}
  }
  static List<Object> rows(Connection c,String sql) throws SQLException {
    List<Object> rows=new ArrayList<>();try(var s=c.createStatement();var r=s.executeQuery(sql)){while(r.next()){List<Object> row=new ArrayList<>();for(int i=1;i<=r.getMetaData().getColumnCount();i++)row.add(r.getObject(i));rows.add(row);}}return rows;
  }
  static Object point(Connection c,int id) throws SQLException {
    try(var s=c.prepareStatement("SELECT id,name,score FROM "+table+" WHERE id=?")){s.setInt(1,id);try(var r=s.executeQuery()){return pointResult(r,id);}}
  }
  static Object updatedPoint(Connection c,int id,boolean updated) throws SQLException {
    try(var s=c.prepareStatement("SELECT id,name,score FROM "+table+" WHERE id=?")) {
      s.setInt(1,id);
      try(var r=s.executeQuery()) {
        require(r.next(),"missing_row");int actualId=r.getInt(1);String name=r.getString(2);long score=r.getLong(3);
        require(!r.wasNull(),"nullability_mismatch");require(actualId==id&&"a".equals(name)&&(!updated || score==25L),"value_mismatch");require(!r.next(),"unexpected_row_count");
        return obj("id",actualId,"name",name,"score",score);
      }
    }
  }
  static boolean exists(Connection c,int id) throws SQLException {
    try(var s=c.prepareStatement("SELECT id FROM "+table+" WHERE id=?")) {
      s.setInt(1,id);
      try(var r=s.executeQuery()) {
        if(!r.next())return false;
        int actualId=r.getInt(1);require(!r.wasNull()&&actualId==id,"nullability_mismatch");require(!r.next(),"unexpected_row_count");return true;
      }
    }
  }
  static Object pointResult(ResultSet r,int id) throws SQLException {
    require(r.next(),"missing_row");List<Object> values=new ArrayList<>(),nulls=new ArrayList<>(),meta=new ArrayList<>();
    var m=r.getMetaData();require(m.getColumnCount()==3,"unexpected_row_count");
    String[] labels={"id","name","score"};
    int[] types={Types.INTEGER,Types.VARCHAR,Types.BIGINT};
    for(int i=1;i<=3;i++) {
      Object v=switch(i){case 1->r.getInt(i);case 2->r.getString(i);default->r.getLong(i);};
      boolean absent=r.wasNull();nulls.add(absent);values.add(absent ? null : v);
      require(labels[i-1].equals(m.getColumnLabel(i)),"column_label_mismatch");require(m.getColumnType(i)==types[i-1],"column_type_mismatch");
      require(m.getColumnTypeName(i)!=null&&!m.getColumnTypeName(i).isEmpty(),"missing_type");
      meta.add(obj("label",m.getColumnLabel(i),"jdbc_type",m.getColumnType(i),"type_name",m.getColumnTypeName(i)));
    }
    require(values.get(0).equals(id),"value_mismatch");require(values.get(1).equals(id==1?"a":"b"),"value_mismatch");
    if(id==1){require(values.get(2).equals(20L),"value_mismatch");require(nulls.equals(List.of(false,false,false)),"nullability_mismatch");}
    if(id==2)require(nulls.equals(List.of(false,false,true)),"nullability_mismatch");
    require(!r.next(),"unexpected_row_count");return obj("values",values,"was_null",nulls,"metadata",meta);
  }
  static String expectState(Connection c,String sql,String expected) throws SQLException {
    try(var s=c.createStatement()){s.execute(sql);throw new ProbeAssertion("expected_error_missing");}catch(SQLException e){if(!expected.equals(e.getSQLState()))throw e;return e.getSQLState();}
  }
  static Object parameter(Connection c,String sql,boolean bytes) throws SQLException {
    try(var s=c.prepareStatement(sql)){if(bytes){s.setBytes(1,new byte[]{2});s.setBytes(2,new byte[]{1,2,3});}else s.setInt(1,7);
      try(var r=s.executeQuery()){require(r.next(),"missing_row");Object value=r.getObject(1);require(value instanceof Number,"column_type_mismatch");require(((Number)value).longValue()==(bytes?2:7),"value_mismatch");Object result=obj("status","OK","value",value,"jdbc_type",r.getMetaData().getColumnType(1));require(!r.next(),"unexpected_row_count");return result;}}
    catch(Throwable e){Map<String,Object> result=new TreeMap<>((Map<String,Object>)error(e));result.put("status","ERR");return result;}
  }
  static Object typePoint(Connection c,String typeTable,int id,Object expected) throws SQLException {
    try(var statement=c.prepareStatement("SELECT id,v FROM "+typeTable+" WHERE id=?")) {
      statement.setInt(1,id);
      try(var result=statement.executeQuery()) {
        require(result.next(),"missing_row");require(result.getInt(1)==id,"value_mismatch");
        Object value=canonical(result.getObject(2));boolean absent=result.wasNull();
        require(Objects.equals(value,expected),"value_mismatch");require(absent==(expected==null),"nullability_mismatch");
        var metadata=result.getMetaData();
        Object data=obj("value",value,"was_null",absent,"jdbc_type",metadata.getColumnType(2),
            "type_name",metadata.getColumnTypeName(2));
        require(!result.next(),"unexpected_row_count");return data;
      }
    }
  }
  static Object typesRoundtrip(Connection c,String prefix) {
    String[] names={"boolean","bytea","numeric","smallint","double","varchar","timestamptz","uuid"};
    String[] declarations={"boolean","bytea","numeric(10,2)","smallint","double precision","varchar(20)","timestamptz","uuid"};
    int[] jdbcTypes={Types.BOOLEAN,Types.BINARY,Types.NUMERIC,Types.SMALLINT,Types.DOUBLE,Types.VARCHAR,Types.TIMESTAMP_WITH_TIMEZONE,Types.OTHER};
    Object[] expected={true,"0001ff","12.30",-12,1.25,"text","2024-01-02T03:04:05Z","123e4567-e89b-12d3-a456-426614174000"};
    Map<String,Object> results=new TreeMap<>();
    for(int index=0;index<names.length;index++) {
      final int type=index;
      String typeTable=prefix+"_t_"+names[type];
      Map<String,Object> steps=new TreeMap<>();
      Map<String,Object> creation=observe(()->{
        try(var statement=c.createStatement()) {
          int count=statement.executeUpdate("CREATE TABLE "+typeTable+" (id integer PRIMARY KEY,v "+declarations[type]+")");
          require(count==0,"update_count_mismatch");return obj("update_count",count);
        }
      });
      steps.put("create",creation);
      if(ok(creation)) {
        for(int row=1;row<=2;row++) {
          final int id=row;
          steps.put("insert_"+id,observe(()->{
            try(var statement=c.prepareStatement("INSERT INTO "+typeTable+" (id,v) VALUES (?,?)")) {
              statement.setInt(1,id);
              if(id==2) statement.setNull(2,jdbcTypes[type]);
              else switch(type) {
                case 0 -> statement.setBoolean(2,true);
                case 1 -> statement.setBytes(2,new byte[]{0,1,(byte)255});
                case 2 -> statement.setBigDecimal(2,new BigDecimal("12.30"));
                case 3 -> statement.setShort(2,(short)-12);
                case 4 -> statement.setDouble(2,1.25);
                case 5 -> statement.setString(2,"text");
                case 6 -> statement.setObject(2,OffsetDateTime.parse("2024-01-02T03:04:05Z"),Types.TIMESTAMP_WITH_TIMEZONE);
                case 7 -> statement.setObject(2,UUID.fromString((String)expected[type]));
                default -> throw new ProbeAssertion("unsupported_type_binding");
              }
              int count=statement.executeUpdate();require(count==1,"update_count_mismatch");return obj("update_count",count);
            }
          }));
          steps.put("point_"+id,observe(()->typePoint(c,typeTable,id,id==1?expected[type]:null)));
        }
      }
      boolean passed=steps.values().stream().allMatch(value->"OK".equals(((Map<?,?>)value).get("status")));
      results.put(names[type],obj("status",passed?"OK":"ERR","steps",steps));
    }
    return results;
  }
  public static void main(String[] args) {
    require("42.7.3".equals(org.postgresql.util.DriverInfo.DRIVER_VERSION),"version_mismatch");
    String prefix=env("R0_PREFIX","r0d_jdbc");require(prefix.matches("[a-z][a-z0-9_]{0,47}"),"invalid_prefix");table=prefix+"_items";
    System.out.println("VERSION\tjdbc\t42.7.3");
    test("connect",c->obj(
        "is_valid",observe(()->{boolean valid=c.isValid(3);require(valid,"connection_invalid");return valid;}),
        "autocommit",observe(()->{boolean value=c.getAutoCommit();require(value,"autocommit_mismatch");return value;}),
        "identity",observe(()->{var result=rows(c,"SELECT current_user,current_database()");require(result.size()==1,"unexpected_row_count");var row=(List<?>)result.get(0);require(row.get(0) instanceof String&&!((String)row.get(0)).isEmpty(),"value_mismatch");require(row.get(1) instanceof String&&!((String)row.get(1)).isEmpty(),"value_mismatch");return result;})));
    test("keepalive",c->obj("statement",observe(()->{Object value=scalar(c,"SELECT 1",false);require(((Map<?,?>)value).get("value").equals(1),"value_mismatch");return value;}),
        "prepared",observe(()->{Object value=scalar(c,"SELECT 1",true);require(((Map<?,?>)value).get("value").equals(1),"value_mismatch");return value;})));
    test("ddl_create",c->{try(var s=c.createStatement()){int n=s.executeUpdate("CREATE TABLE "+table+" (id integer PRIMARY KEY,name text,score bigint)");require(n==0,"update_count_mismatch");return obj("update_count",n);}});
    test("prepared_insert",c->{
      Map<String,Object> inserts=new TreeMap<>();
      for(int row=1;row<=3;row++) {
        final int id=row;
        inserts.put("row_"+id,observe(()->{
          try(var statement=c.prepareStatement("INSERT INTO "+table+" (id,name,score) VALUES (?,?,?)")) {
            statement.setInt(1,id);statement.setString(2,new String[]{"a","b","c"}[id-1]);
            if(id==2)statement.setNull(3,Types.BIGINT);else statement.setLong(3,20L);
            int count=statement.executeUpdate();require(count==1,"update_count_mismatch");return obj("update_count",count);
          }
        }));
      }
      return inserts;
    });
    test("prepared_point_select",c->obj("row",observe(()->point(c,1)),"null_row",observe(()->point(c,2))));
    test("prepared_repeat",c->{Object first=null,last=null;try(var s=c.prepareStatement("SELECT id,name,score FROM "+table+" WHERE id=?")){s.setInt(1,1);for(int i=0;i<7;i++){try(var r=s.executeQuery()){last=pointResult(r,1);if(i==0)first=last;else require(first.equals(last),"value_mismatch");}}}return obj("executions",7,"identical",true,"row",last);});
    test("select_ordered",c->{var result=rows(c,"SELECT id,name FROM "+table+" ORDER BY id");require(result.equals(List.of(List.of(1,"a"),List.of(2,"b"),List.of(3,"c"))),"value_mismatch");return obj("rows",result);});
    test("update_delete_params",c->{
      var update=observe(()->{try(var statement=c.prepareStatement("UPDATE "+table+" SET score=score+? WHERE id=?")){statement.setLong(1,5);statement.setInt(2,1);int count=statement.executeUpdate();require(count==1,"update_count_mismatch");return obj("update_count",count);}});
      var delete=observe(()->{try(var statement=c.prepareStatement("DELETE FROM "+table+" WHERE id=?")){statement.setInt(1,3);int count=statement.executeUpdate();require(count==1,"update_count_mismatch");return obj("delete_count",count);}});
      return obj("update",update,"delete",delete,"point",observe(()->updatedPoint(c,1,ok(update))));
    });
    test("tx_commit",c->{
      Map<String,Object> steps=new TreeMap<>();
      var begin=observe(()->{c.setAutoCommit(false);return true;});steps.put("begin",begin);
      if(ok(begin)) {
        steps.put("insert",observe(()->insert(c,10)));
        steps.put("commit",observe(()->{c.commit();return true;}));
      }
      steps.put("other_connection",observe(()->{
        try(var other=connect()) {
          var result=rows(other,"SELECT id,name FROM "+table+" WHERE id=10");
          require(result.equals(List.of(List.of(10,"tx10"))),"value_mismatch");return obj("rows",result);
        }
      }));
      return steps;
    });
    test("tx_rollback",c->{
      Map<String,Object> steps=new TreeMap<>();
      var begin=observe(()->{c.setAutoCommit(false);return true;});steps.put("begin",begin);
      if(ok(begin)) {
        steps.put("insert",observe(()->insert(c,11)));
        steps.put("rollback",observe(()->{c.rollback();return true;}));
      }
      steps.put("point",observe(()->{var result=rows(c,"SELECT id FROM "+table+" WHERE id=11");require(result.isEmpty(),"value_mismatch");return obj("rows",result);}));
      return steps;
    });
    test("tx_error_recovery",c->{c.setAutoCommit(false);
      var duplicate=observe(()->obj("sqlstate",expectState(c,"INSERT INTO "+table+" (id,name) VALUES (1,'duplicate')","23505")));
      var aborted=observe(()->obj("sqlstate",expectState(c,"SELECT 1","25P02")));
      var rollback=observe(()->{c.rollback();return true;});
      var recovered=observe(()->{Object value=scalar(c,"SELECT 1",false);require(((Map<?,?>)value).get("value").equals(1),"value_mismatch");return value;});
      return obj("duplicate",duplicate,"aborted",aborted,"rollback",rollback,"recovered",recovered);
    });
    test("savepoint",c->{c.setAutoCommit(false);Map<String,Object> steps=new TreeMap<>();
      steps.put("insert_a",observe(()->insert(c,20)));
      Savepoint[] savepoint={null};
      var saved=observe(()->{savepoint[0]=c.setSavepoint();return true;});steps.put("set_savepoint",saved);
      if(ok(saved)) {
        steps.put("insert_b",observe(()->insert(c,21)));
        var rollback=observe(()->{c.rollback(savepoint[0]);return true;});steps.put("rollback_savepoint",rollback);
        if(ok(rollback))steps.put("commit",observe(()->{c.commit();return true;}));
        else steps.put("rollback_all",observe(()->{c.rollback();return true;}));
      } else steps.put("rollback_all",observe(()->{c.rollback();return true;}));
      steps.put("point_a",observe(()->{boolean present=exists(c,20);require(present,"value_mismatch");return obj("exists",present);}));
      steps.put("point_b",observe(()->{boolean present=exists(c,21);require(!present,"value_mismatch");return obj("exists",present);}));
      return steps;
    });
    test("metadata_tables",c->{List<Object> result=new ArrayList<>();try(var r=c.getMetaData().getTables(null,"public",prefix+"_items",new String[]{"TABLE"})){while(r.next())result.add(List.of(r.getString("TABLE_SCHEM"),r.getString("TABLE_NAME"),r.getString("TABLE_TYPE")));}result.sort(Comparator.comparing(JdbcCompatProbe::json));require(result.equals(List.of(List.of("public",prefix+"_items","TABLE"))),"value_mismatch");return obj("rows",result);});
    test("metadata_columns",c->{List<Map<String,Object>> result=new ArrayList<>();try(var r=c.getMetaData().getColumns(null,"public",prefix+"_items","%")){while(r.next())result.add(obj("name",r.getString("COLUMN_NAME"),"jdbc_type",r.getInt("DATA_TYPE"),"type_name",r.getString("TYPE_NAME"),"nullable",r.getInt("NULLABLE"),"ordinal",r.getInt("ORDINAL_POSITION")));}result.sort(Comparator.<Map<String,Object>>comparingInt(x->(Integer)x.get("ordinal")).thenComparing(x->(String)x.get("name")));require(result.size()==3,"unexpected_row_count");String[] names={"id","name","score"};int[] types={Types.INTEGER,Types.VARCHAR,Types.BIGINT};for(int i=0;i<3;i++){require(names[i].equals(result.get(i).get("name")),"column_label_mismatch");require(result.get(i).get("jdbc_type").equals(types[i]),"column_type_mismatch");require(result.get(i).get("nullable").equals(i==0?DatabaseMetaData.columnNoNulls:DatabaseMetaData.columnNullable),"nullability_mismatch");}return obj("columns",result);});
    test("metadata_pk",c->{List<Map<String,Object>> result=new ArrayList<>();try(var r=c.getMetaData().getPrimaryKeys(null,"public",prefix+"_items")){while(r.next()){String name=r.getString("PK_NAME");result.add(obj("column",r.getString("COLUMN_NAME"),"key_seq",r.getInt("KEY_SEQ"),"pk_name_nonempty",name!=null&&!name.isEmpty()));}}result.sort(Comparator.comparingInt(x->(Integer)x.get("key_seq")));require(result.size()==1,"unexpected_row_count");require(result.get(0).get("column").equals("id"),"value_mismatch");require(result.get(0).get("key_seq").equals(1),"value_mismatch");require(result.get(0).get("pk_name_nonempty").equals(true),"value_mismatch");return obj("keys",result);});
    test("metadata_typeinfo",c->{
      Set<String> wanted=Set.of("int2","int4","int8","text","bool","bytea","numeric","varchar");
      Map<String,Object> result=new TreeMap<>();
      var read=observe(()->{
        try(var rows=c.getMetaData().getTypeInfo()) {
          while(rows.next()) {
            String name=rows.getString("TYPE_NAME");
            if(wanted.contains(name)) {
              require(!result.containsKey(name),"duplicate_type");result.put(name,rows.getInt("DATA_TYPE"));
            }
          }
        }
        return true;
      });
      var required=observe(()->{require(result.keySet().equals(wanted),"missing_type");return true;});
      return obj("observed_types",result,"read",read,"required_types",required);
    });
    test("param_bytea_position",c->obj("cast",parameter(c,"SELECT position(?::bytea IN ?::bytea)",true),"uncast",parameter(c,"SELECT position(? IN ?)",true)));
    test("param_no_from_aggregate",c->{Object count;try{count=scalar(c,"SELECT count(*)",false);require(((Number)((Map<?,?>)count).get("value")).longValue()==1,"value_mismatch");Map<String,Object> v=new TreeMap<>((Map<String,Object>)count);v.put("status","OK");count=v;}catch(Throwable e){Map<String,Object> v=new TreeMap<>((Map<String,Object>)error(e));v.put("status","ERR");count=v;}return obj("count",count,"max",parameter(c,"SELECT max(?)",false));});
    test("types_roundtrip",c->typesRoundtrip(c,prefix));
    System.out.println("DONE\t19");
  }
}
