import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import java.security.cert.*;
import javax.net.ssl.*;

/** Actual pgJDBC probe. Every PASS follows assertions, never merely lack of an exception. */
public class JdbcProbe {
  static String fixture, port, profile;
  public static class TLS extends org.postgresql.ssl.WrappedFactory {
    public TLS() throws Exception {
      Path root=Path.of(System.getProperty("fixture"));
      CertificateFactory cf=CertificateFactory.getInstance("X.509");
      java.security.cert.Certificate ca, cert;
      try(var in=Files.newInputStream(root.resolve("ca.pem"))){ca=cf.generateCertificate(in);}
      try(var in=Files.newInputStream(root.resolve("app.pem"))){cert=cf.generateCertificate(in);}
      String pem=Files.readString(root.resolve("app.key.TEST-ONLY"));
      byte[] der=Base64.getDecoder().decode(pem.replaceAll("-----[^-]+-----", "").replaceAll("\\s", ""));
      var key=KeyFactory.getInstance("EC").generatePrivate(new PKCS8EncodedKeySpec(der));
      KeyStore keys=KeyStore.getInstance("PKCS12");keys.load(null,null);
      keys.setKeyEntry("user",key,new char[0],new java.security.cert.Certificate[]{cert,ca});
      KeyManagerFactory km=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());km.init(keys,new char[0]);
      KeyStore trust=KeyStore.getInstance("PKCS12");trust.load(null,null);trust.setCertificateEntry("ca",ca);
      TrustManagerFactory tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());tm.init(trust);
      SSLContext context=SSLContext.getInstance("TLS");context.init(km.getKeyManagers(),tm.getTrustManagers(),null);factory=context.getSocketFactory();
    }
  }
  interface Check { void run(Connection c) throws Exception; }
  static void require(boolean b,String reason){if(!b)throw new AssertionError(reason);}
  static Connection connect() throws Exception {
    Properties p=new Properties();p.setProperty("user","giggles");p.setProperty("sslmode","verify-full");
    p.setProperty("sslfactory","JdbcProbe$TLS");p.setProperty("connectTimeout","5");p.setProperty("socketTimeout","5");
    if(profile.equals("configured"))p.setProperty("assumeMinServerVersion","14");
    return DriverManager.getConnection("jdbc:postgresql://localhost:"+port+"/giggles",p);
  }
  static void emit(String id,String status,String actual){
    if(actual.length()>3000)actual=actual.substring(0,3000);
    System.out.println("CASE\t"+id+"\t"+status+"\t"+Base64.getEncoder().encodeToString(actual.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  }
  static void test(String id, Check f){
    try(Connection c=connect()){f.run(c);emit(id,"PASS","assertions satisfied");}
    catch(Throwable e){String state=e instanceof SQLException?((SQLException)e).getSQLState():"";emit(id,"FAIL","SQLSTATE="+state+" "+e.getClass().getSimpleName()+": "+e.getMessage());}
  }
  static void empty(Connection c,long id) throws Exception {
    try(var q=c.prepareStatement("SELECT id,body FROM app WHERE id=?")){q.setLong(1,id);try(var r=q.executeQuery()){require(!r.next(),"expected no rows for "+id);}}
  }
  static void insert(Connection c,long id,String body) throws Exception {
    try(var q=c.prepareStatement("INSERT INTO app (id,body) VALUES (?,?)")){q.setLong(1,id);q.setString(2,body);require(q.executeUpdate()==1,"insert count");}
  }
  static void body(Connection c,long id,String text) throws Exception {
    try(var q=c.prepareStatement("SELECT id,body FROM app WHERE id=?")){q.setLong(1,id);try(var r=q.executeQuery()){require(r.next(),"missing row");require(r.getLong(1)==id,"wrong id");require(Objects.equals(r.getString(2),text),"wrong body/null");require(!r.next(),"extra row");}}
  }
  public static void main(String[] args) throws Exception {
    port=args[0];profile=args[1];
    String v=org.postgresql.util.DriverInfo.DRIVER_VERSION;System.out.println("VERSION\t"+v);
    if(!v.equals("42.7.3"))throw new IllegalStateException("expected pgJDBC 42.7.3, got "+v);
    test("connect_init",c->{require(!c.isClosed(),"closed");require(c.getAutoCommit(),"autocommit default");});
    test("is_valid",c->require(c.isValid(3),"isValid false"));
    test("simple_query",c->{try(var s=c.createStatement();var r=s.executeQuery("SELECT id,body FROM app WHERE id=8000")){require(!r.next(),"fixture must have no 8000 row");}});
    test("prepared_empty",c->empty(c,8000));
    test("type_bigint",c->{insert(c,9000000000L,"bigint");body(c,9000000000L,"bigint");});
    test("type_text",c->{insert(c,8107,"hello λ");body(c,8107,"hello λ");});
    for(String col:List.of("flag","small","num","data"))test("type_"+col,c->{
      long id=8110+List.of("flag","small","num","data").indexOf(col);
      try(var q=c.prepareStatement("INSERT INTO app (id,"+col+") VALUES (?,?)")){
        q.setLong(1,id);switch(col){case "flag":q.setBoolean(2,false);break;case "small":q.setShort(2,(short)-12);break;case "num":q.setInt(2,-12345);break;case "data":q.setBytes(2,new byte[]{0,127,-1});break;}
        try{require(q.executeUpdate()==1,"insert count");}catch(SQLException e){throw new SQLException("insert "+col+": "+e.getMessage(),e.getSQLState(),e);}}
      try(var q=c.prepareStatement("SELECT "+col+" FROM app WHERE id=?")){q.setLong(1,id);for(int i=0;i<7;i++){
        try(var r=q.executeQuery()){require(r.next(),"row missing "+col);switch(col){case "flag":require(!r.getBoolean(1)&&!r.wasNull(),"boolean");break;case "small":require(r.getShort(1)==-12,"smallint");break;case "num":require(r.getInt(1)==-12345,"integer");break;case "data":require(Arrays.equals(r.getBytes(1),new byte[]{0,127,-1}),"bytes");break;}require(!r.next(),"extra row");}
        catch(SQLException e){throw new SQLException("query "+col+" iteration "+i+": "+e.getMessage(),e.getSQLState(),e);}
      }}
    });
    test("typed_values",c->{
      try(var q=c.prepareStatement("INSERT INTO app (id,body,flag,small,num,data) VALUES (?,?,?,?,?,?)")){
        q.setLong(1,8101);q.setString(2,"hello λ");q.setBoolean(3,false);q.setShort(4,(short)-12);q.setInt(5,-12345);q.setBytes(6,new byte[]{0,127,-1});require(q.executeUpdate()==1,"typed insert count");}
      // Repetition crosses pgJDBC's prepare threshold and exercises binary results.
      try(var q=c.prepareStatement("SELECT id,body,flag,small,num,data FROM app WHERE id=?")){q.setLong(1,8101);
        for(int i=0;i<7;i++)try(var r=q.executeQuery()){
          require(r.next(),"typed row missing");require(r.getLong(1)==8101,"BIGINT");require(r.getString(2).equals("hello λ"),"TEXT");
          require(!r.getBoolean(3)&&!r.wasNull(),"BOOLEAN");require(r.getShort(4)==-12,"SMALLINT");require(r.getInt(5)==-12345,"INTEGER");
          require(Arrays.equals(r.getBytes(6),new byte[]{0,127,-1}),"BYTEA");require(!r.next(),"extra typed row");}}
    });
    test("null_values",c->{insert(c,8102,null);try(var q=c.prepareStatement("SELECT body,flag,small,num,data FROM app WHERE id=?")){q.setLong(1,8102);try(var r=q.executeQuery()){
      require(r.next(),"null row missing");require(r.getString(1)==null&&r.wasNull(),"text null");require(!r.getBoolean(2)&&r.wasNull(),"bool null");require(r.getShort(3)==0&&r.wasNull(),"small null");require(r.getInt(4)==0&&r.wasNull(),"int null");require(r.getBytes(5)==null&&r.wasNull(),"bytes null");require(!r.next(),"extra null row");}}});
    test("rollback",c->{c.setAutoCommit(false);insert(c,8103,"rollback");c.rollback();c.setAutoCommit(true);empty(c,8103);});
    test("commit",c->{c.setAutoCommit(false);insert(c,8104,"commit");c.commit();c.setAutoCommit(true);try(var other=connect()){body(other,8104,"commit");}});
    test("error_recovery",c->{
      c.setAutoCommit(false);insert(c,8105,"first");
      try{insert(c,8105,"duplicate");throw new AssertionError("duplicate succeeded");}catch(SQLException e){require("23505".equals(e.getSQLState()),"duplicate SQLSTATE="+e.getSQLState());}
      try{empty(c,8000);throw new AssertionError("failed transaction accepted query");}catch(SQLException e){require("25P02".equals(e.getSQLState()),"failed tx SQLSTATE="+e.getSQLState());}
      c.rollback();c.setAutoCommit(true);empty(c,8105);empty(c,8000);
    });
    test("authorization",c->{try(var s=c.createStatement()){
      try(var r=s.executeQuery("SELECT id,body FROM other")){throw new AssertionError("unauthorized table readable");}
      catch(SQLException e){require("42501".equals(e.getSQLState()),"authorization SQLSTATE="+e.getSQLState());}}
    });
    test("metadata_tables",c->{Set<String> names=new TreeSet<>();try(var r=c.getMetaData().getTables(null,null,"%",new String[]{"TABLE"})){while(r.next()){require(names.size()<100,"table bound");names.add(r.getString("TABLE_NAME"));}}require(names.equals(Set.of("app")),"expected visible app only; actual="+names);});
    test("metadata_columns",c->{Map<String,Integer> cols=new TreeMap<>();try(var r=c.getMetaData().getColumns(null,null,"app","%")){while(r.next()){require(cols.size()<100,"column bound");cols.put(r.getString("COLUMN_NAME"),r.getInt("DATA_TYPE"));}}
      require(cols.equals(Map.of("id",Types.BIGINT,"body",Types.VARCHAR,"flag",Types.BOOLEAN,"small",Types.SMALLINT,"num",Types.INTEGER,"data",Types.BINARY)),"column types="+cols);});
    test("metadata_pk",c->{List<String> keys=new ArrayList<>();try(var r=c.getMetaData().getPrimaryKeys(null,null,"app")){while(r.next()){require(keys.size()<100,"pk bound");keys.add(r.getString("COLUMN_NAME"));}}require(keys.equals(List.of("id")),"PK="+keys);});
    test("metadata_types",c->{Set<Integer> types=new HashSet<>();int count=0;try(var r=c.getMetaData().getTypeInfo()){while(r.next()){require(++count<1000,"type bound");types.add(r.getInt("DATA_TYPE"));}}require(types.containsAll(Set.of(Types.BIGINT,Types.VARCHAR,Types.BOOLEAN,Types.SMALLINT,Types.INTEGER,Types.BINARY)),"type map="+types);});
    test("metadata_hidden",c->{try(var r=c.getMetaData().getColumns(null,null,"other","%")){require(!r.next(),"unauthorized metadata disclosed");}});
    test("repeat_prepare_close",c->{for(int i=0;i<12;i++)empty(c,8000);});
    test("close",c->{c.close();require(c.isClosed(),"close not reflected");require(!c.isValid(1),"closed connection valid");});
    emit("schema_fence","UNTESTED","no runner-owned administrative schema-change channel");
    emit("cancel","UNTESTED","no bounded long-running authorized query fixture; authenticated CancelRequest contract requires separate control");
  }
}
