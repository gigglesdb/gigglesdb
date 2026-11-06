import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import java.security.cert.*;
import javax.net.ssl.*;

/** Actual pgJDBC probe. Every PASS follows assertions, never merely lack of an exception. */
public class JdbcProbe {
  static String fixture, port, profile, activeCase="startup";
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
    activeCase=id;
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
  // Captured expanded SQL is immutable; expectations below describe the fixture independently.
  static void batchQuery(String id,String sql,String[] labels,String[] types,CheckResult check){
    test(id,c->{for(boolean prepared:new boolean[]{false,true}){
      try(Statement st=prepared?c.prepareStatement(sql):c.createStatement();
          ResultSet r=prepared?((PreparedStatement)st).executeQuery():st.executeQuery(sql)){
        metadata(r,labels,types);
        check.run(r);
      }
    }});
  }
  static void metadata(ResultSet r,String[] labels,String[] types) throws Exception {
    ResultSetMetaData m=r.getMetaData();require(m.getColumnCount()==labels.length,"column count="+m.getColumnCount());
    for(int i=1;i<=labels.length;i++){
      require(labels[i-1].equals(m.getColumnLabel(i)),"label "+i+"="+m.getColumnLabel(i));
      require(types[i-1].equals(m.getColumnTypeName(i)),"type name "+i+"="+m.getColumnTypeName(i));
      require(m.getColumnType(i)==jdbcType(types[i-1]),"JDBC type "+i+"="+m.getColumnType(i));
    }
  }
  record Fixture(long schema,long table,long index,long constraint) {}
  static long oid(Connection c,String sql) throws Exception {
    try(Statement s=c.createStatement();ResultSet r=s.executeQuery(sql)){
      require(r.next(),"missing fixture object: "+sql);long id=r.getLong(1);require(id>0&&!r.wasNull(),"invalid fixture oid");require(!r.next(),"duplicate fixture object");return id;
    }
  }
  static Fixture fixture(Connection c) throws Exception {
    long schema=oid(c,"select oid from pg_catalog.pg_namespace where nspname='public'");
    long table=oid(c,"select oid from pg_catalog.pg_class where relnamespace="+schema+" and relname='app' and relkind='r'");
    long index=oid(c,"select indexrelid from pg_catalog.pg_index where indrelid="+table+" and indisprimary");
    long constraint=oid(c,"select oid from pg_catalog.pg_constraint where conrelid="+table+" and contype='p'");
    require(new HashSet<Long>(List.of(schema,table,index,constraint)).size()==4,"object oid collisions");
    try(var st=c.createStatement();var r=st.executeQuery("select oid from pg_catalog.pg_class where relname='other'")){require(!r.next(),"hidden other catalog disclosed");}
    return new Fixture(schema,table,index,constraint);
  }
  interface CheckFixture { void run(ResultSet r,Fixture f) throws Exception; }
  static void appQuery(String id,String template,String[] labels,String[] types,CheckFixture check){
    test(id,c->{Fixture f=fixture(c);String sql=template.replace(":schema_id",Long.toString(f.schema()));
      for(boolean prepared:new boolean[]{false,true}){
        try(Statement st=prepared?c.prepareStatement(sql):c.createStatement();ResultSet r=prepared?((PreparedStatement)st).executeQuery():st.executeQuery(sql)){
          metadata(r,labels,types);check.run(r,f);
        }
      }
    });
  }
  static void nulls(ResultSet r,String... fields) throws Exception {for(String field:fields)require(r.getObject(field)==null&&r.wasNull(),"invented "+field);}
  static void number(ResultSet r,String field,long value) throws Exception {require(r.getLong(field)==value&&!r.wasNull(),field+" expected "+value);}
  static void flag(ResultSet r,String field,boolean value) throws Exception {require(r.getBoolean(field)==value&&!r.wasNull(),field+" expected "+value);}
  static void one(ResultSet r) throws Exception {require(r.next(),"missing fixture row");}
  static void last(ResultSet r) throws Exception {require(!r.next(),"extra fixture row or hidden object");}
  static void discoveryApp(){
    appQuery("dg_App_RetrieveExistentTables","select oid from pg_catalog.pg_class C\nwhere relkind in ('r', 'm', 'v', 'p', 'f')\n  and relnamespace = :schema_id::oid\n;",new String[]{"oid"},new String[]{"oid"},(r,f)->{one(r);number(r,"oid",f.table());last(r);});
    appQuery("dg_App_RetrieveTables","select T.relkind as table_kind,\n       T.relname as table_name,\n       T.oid as table_id,\n       T.xmin as table_state_number,\n       false as table_with_oids,\n       T.reltablespace as tablespace_id,\n       T.reloptions as options,\n       T.relpersistence as persistence,\n       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors,\n       (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors,\n       T.relispartition as is_partition,\n       pg_catalog.pg_get_partkeydef(T.oid) as partition_key,\n       pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression,\n       T.relam am_id,\n       pg_catalog.pg_get_userbyid(T.relowner) as \"owner\"\nfrom pg_catalog.pg_class T\nwhere relnamespace = :schema_id::oid\n       and relkind in ('r', 'm', 'v', 'f', 'p')\n\n\norder by table_kind, table_id\n;",new String[]{"table_kind","table_name","table_id","table_state_number","table_with_oids","tablespace_id","options","persistence","ancestors","successors","is_partition","partition_key","partition_expression","am_id","owner"},new String[]{"char","name","oid","xid","bool","oid","_text","char","text","text","bool","text","text","oid","name"},(r,f)->{one(r);number(r,"table_id",f.table());require("app".equals(r.getString("table_name")),"table name");require("r".equals(r.getString("table_kind")),"table kind");require("p".equals(r.getString("persistence")),"persistent table");require("giggles".equals(r.getString("owner")),"table owner");flag(r,"table_with_oids",false);flag(r,"is_partition",false);number(r,"tablespace_id",0);number(r,"am_id",0);nulls(r,"table_state_number","options","ancestors","successors","partition_key","partition_expression");last(r);});
    appQuery("dg_App_RetrieveColumns","with T as ( select \n                  T.oid as table_id, T.relname as table_name\n            from pg_catalog.pg_class T\n                 \n            where T.relnamespace = :schema_id::oid\n              and T.relkind in ('r', 'm', 'v', 'f', 'p')\n              \n              \n             \n            )\nselect T.table_id,\n       C.attnum as column_position,\n       C.attname as column_name,\n       C.xmin as column_state_number,\n       C.atttypmod as type_mod,\n       C.attndims as dimensions_number,\n       pg_catalog.format_type(C.atttypid, C.atttypmod) as type_spec,\n       C.atttypid as type_id,\n       C.attnotnull as mandatory,\n       pg_catalog.pg_get_expr(D.adbin, T.table_id) as column_default_expression,\n       not C.attislocal as column_is_inherited,\n        C.attfdwoptions as options,\n       C.attisdropped as column_is_dropped,\n       C.attidentity as identity_kind,\n       C.attgenerated as generated\nfrom T\n  join pg_catalog.pg_attribute C on T.table_id = C.attrelid\n  left join pg_catalog.pg_attrdef D on (C.attrelid, C.attnum) = (D.adrelid, D.adnum)\nwhere attnum > 0\norder by table_id, attnum\n;",new String[]{"table_id","column_position","column_name","column_state_number","type_mod","dimensions_number","type_spec","type_id","mandatory","column_default_expression","column_is_inherited","options","column_is_dropped","identity_kind","generated"},new String[]{"oid","int2","name","xid","int4","int4","text","oid","bool","text","bool","_text","bool","char","char"},(r,f)->{String[] names={"id","body","flag","small","num","data"};String[] specs={"bigint","text","boolean","smallint","integer","bytea"};int[] ids={20,25,16,21,23,17};for(int i=0;i<names.length;i++){one(r);number(r,"table_id",f.table());number(r,"column_position",i+1);require(names[i].equals(r.getString("column_name")),"column name/order");require(specs[i].equals(r.getString("type_spec")),"column type specification");number(r,"type_id",ids[i]);number(r,"type_mod",-1);number(r,"dimensions_number",0);flag(r,"mandatory",i==0);flag(r,"column_is_inherited",false);flag(r,"column_is_dropped",false);require("".equals(r.getString("identity_kind")),"identity unsupported");require("".equals(r.getString("generated")),"generated unsupported");nulls(r,"column_state_number","column_default_expression","options");}last(r);});
    appQuery("dg_App_RetrieveIndices","select tab.oid               table_id,\n       tab.relkind           table_kind,\n       ind_stor.relname      index_name,\n       ind_head.indexrelid   index_id,\n       ind_stor.xmin         state_number,\n       ind_head.indisunique  is_unique,\n       ind_head.indisprimary is_primary,\n       false nulls_not_distinct,\n       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,\n       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,\n       ind_stor.reltablespace tablespace_id,\n       opcmethod as access_method_id\nfrom pg_catalog.pg_class tab\n         join pg_catalog.pg_index ind_head\n              on ind_head.indrelid = tab.oid\n         join pg_catalog.pg_class ind_stor\n              on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid\n         left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)\nwhere tab.relnamespace = :schema_id::oid\n        and tab.relkind in ('r', 'm', 'v', 'p')\n        and ind_stor.relkind in ('i', 'I')\n\n",new String[]{"table_id","table_kind","index_name","index_id","state_number","is_unique","is_primary","nulls_not_distinct","condition","ancestors","tablespace_id","access_method_id"},new String[]{"oid","char","name","oid","xid","bool","bool","bool","text","text","oid","oid"},(r,f)->{one(r);number(r,"table_id",f.table());number(r,"index_id",f.index());require("r".equals(r.getString("table_kind")),"index parent kind");require("app_pkey".equals(r.getString("index_name")),"index name");flag(r,"is_unique",true);flag(r,"is_primary",true);flag(r,"nulls_not_distinct",false);number(r,"tablespace_id",0);nulls(r,"state_number","condition","ancestors","access_method_id");last(r);});
    appQuery("dg_App_RetrieveConstraints","select T.oid table_id,\n       relkind table_kind,\n       C.oid::bigint con_id,\n       C.xmin::varchar::bigint con_state_id,\n       conname con_name,\n       contype con_kind,\n       conkey con_columns,\n       conindid index_id,\n       confrelid ref_table_id,\n       condeferrable is_deferrable,\n       condeferred is_init_deferred,\n       false is_not_enforced,\n       false is_not_validated,\n       false is_defined_with_period,\n       confupdtype on_update,\n       confdeltype on_delete,\n       confmatchtype match_type,\n        connoinherit no_inherit,\n       pg_catalog.pg_get_expr(conbin, T.oid) con_expression,\n       confkey ref_columns,\n       conexclop::int[] excl_operators,\n       array(select unnest::regoper::varchar from unnest(conexclop)) excl_operators_str\nfrom pg_catalog.pg_constraint C\n         join pg_catalog.pg_class T\n              on C.conrelid = T.oid\nwhere relkind in ('r', 'v', 'f', 'p')\n  and relnamespace = :schema_id::oid\n  and contype in ('p', 'u', 'f', 'c', 'x')\n  and connamespace = :schema_id::oid\n  \n;",new String[]{"table_id","table_kind","con_id","con_state_id","con_name","con_kind","con_columns","index_id","ref_table_id","is_deferrable","is_init_deferred","is_not_enforced","is_not_validated","is_defined_with_period","on_update","on_delete","match_type","no_inherit","con_expression","ref_columns","excl_operators","excl_operators_str"},new String[]{"oid","char","int8","int8","name","char","_int2","oid","oid","bool","bool","bool","bool","bool","char","char","char","bool","text","_int2","_int4","_text"},(r,f)->{one(r);number(r,"table_id",f.table());number(r,"con_id",f.constraint());number(r,"index_id",f.index());require("r".equals(r.getString("table_kind")),"constraint parent kind");require("app_pkey".equals(r.getString("con_name")),"PK name");require("p".equals(r.getString("con_kind")),"PK kind");require("{1}".equals(r.getString("con_columns")),"PK id column");number(r,"ref_table_id",0);for(String col:List.of("is_deferrable","is_init_deferred","is_not_enforced","is_not_validated","is_defined_with_period"))flag(r,col,false);flag(r,"no_inherit",true);nulls(r,"con_state_id","con_expression","ref_columns","excl_operators");require("{}".equals(r.getString("excl_operators_str")),"no exclusion operators");last(r);});
    appQuery("dg_App_RetrieveExistentConstraints","select oid\nfrom pg_catalog.pg_constraint\nwhere conrelid != 0 and connamespace = :schema_id::oid\n;",new String[]{"oid"},new String[]{"oid"},(r,f)->{one(r);number(r,"oid",f.constraint());last(r);});
    appQuery("dg_App_RetrieveIndexColumns","\nselect ind_head.indexrelid index_id,\n       k col_idx,\n       k <= indnkeyatts in_key,\n       ind_head.indkey[k-1] column_position,\n       ind_head.indoption[k-1] column_options,\n       ind_head.indcollation[k-1] as collation,\n       colln.nspname as collation_schema,\n       collname as collation_str,\n       ind_head.indclass[k-1] as opclass,\n       case when opcdefault then null else opcn.nspname end as opclass_schema,\n       case when opcdefault then null else opcname end as opclass_str,\n       case\n           when indexprs is null then null\n           when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)\n           else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)\n       end as expression,\n       amcanorder can_order\nfrom pg_catalog.pg_index ind_head\n         join pg_catalog.pg_class ind_stor\n              on ind_stor.oid = ind_head.indexrelid\n    cross join unnest(ind_head.indkey) with ordinality u(u, k)\n         left join pg_catalog.pg_collation\n                   on pg_collation.oid = ind_head.indcollation[k-1]\n         left join pg_catalog.pg_namespace colln on collnamespace = colln.oid\ncross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder\n         left join pg_catalog.pg_opclass\n                   on pg_opclass.oid = ind_head.indclass[k-1]\n         left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid\n  \nwhere ind_stor.relnamespace = :schema_id::oid\n  and ind_stor.relkind in ('i', 'I')\n\norder by index_id, k",new String[]{"index_id","col_idx","in_key","column_position","column_options","collation","collation_schema","collation_str","opclass","opclass_schema","opclass_str","expression","can_order"},new String[]{"oid","int8","bool","int2","int2","oid","name","name","oid","name","name","text","bool"},(r,f)->{one(r);number(r,"index_id",f.index());number(r,"col_idx",1);flag(r,"in_key",true);number(r,"column_position",1);number(r,"column_options",0);number(r,"collation",0);number(r,"opclass",0);nulls(r,"collation_schema","collation_str","opclass_schema","opclass_str","expression","can_order");last(r);});
  }
  static int jdbcType(String type){return switch(type){
    case "oid","int8" -> Types.BIGINT; case "int4" -> Types.INTEGER;
    case "int2" -> Types.SMALLINT; case "bool" -> Types.BIT;
    case "text","name","varchar" -> Types.VARCHAR; case "char" -> Types.CHAR;
    case "_text","_varchar","_aclitem","_int2","_int4","_oid" -> Types.ARRAY;
    case "xid" -> Types.OTHER; default -> throw new AssertionError("unspecified metadata type "+type);
  };}
  interface CheckResult { void run(ResultSet r) throws Exception; }
  static void absent(ResultSet r) throws Exception {require(!r.next(),"invented unsupported object");}
  static void noMVCC(String id,String sql){
    activeCase=id;
    try(Connection c=connect();Statement s=c.createStatement()){
      try(ResultSet r=s.executeQuery(sql)){throw new AssertionError("unsupported PostgreSQL MVCC query succeeded");}
      catch(SQLException e){require("0A000".equals(e.getSQLState()),"unexpected rejection "+e.getSQLState());
        emit(id,"UNTESTED","UNSUPPORTED PostgreSQL MVCC; asserted SQLSTATE=0A000; rejection is not support");}
    }catch(Throwable e){emit(id,"FAIL",e.getClass().getSimpleName()+": "+e.getMessage());}
  }
  static void discoveryBatch(){
    batchQuery("dg_Batch_DateStyle","show DateStyle",new String[]{"DateStyle"},new String[]{"text"},r->{require(r.next(),"missing DateStyle");require("ISO".equals(r.getString(1)),"actual supported DateStyle");require(!r.next(),"extra DateStyle");});
    batchQuery("dg_Batch_RetrieveTimeZones","select name, is_dst from pg_catalog.pg_timezone_names\nunion distinct\nselect abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs",new String[]{"name","is_dst"},new String[]{"text","bool"},JdbcProbe::absent);
    batchQuery("dg_Batch_RetrieveRoles","select R.oid::bigint as role_id, rolname as role_name,\n  rolsuper is_super, rolinherit is_inherit,\n  rolcreaterole can_createrole, rolcreatedb can_createdb,\n  rolcanlogin can_login, rolreplication /* false */ is_replication,\n  rolconnlimit conn_limit, rolvaliduntil valid_until,\n  rolbypassrls /* false */ bypass_rls, rolconfig config,\n  D.description\nfrom pg_catalog.pg_roles R\n  left join pg_catalog.pg_shdescription D on D.objoid = R.oid",new String[]{"role_id","role_name","is_super","is_inherit","can_createrole","can_createdb","can_login","is_replication","conn_limit","valid_until","bypass_rls","config","description"},new String[]{"int8","name","bool","bool","bool","bool","bool","bool","int4","text","bool","_text","text"},r->{require(r.next(),"missing role");require(r.getLong("role_id")>0&&!r.wasNull(),"role oid");require("giggles".equals(r.getString("role_name")),"role identity");for(String col:List.of("is_super","is_inherit","can_createrole","can_createdb","is_replication","bypass_rls")){require(!r.getBoolean(col)&&!r.wasNull(),"unsupported role privilege "+col);}require(r.getBoolean("can_login")&&!r.wasNull(),"application login");for(String col:List.of("conn_limit","valid_until","config","description")){require(r.getObject(col)==null&&r.wasNull(),"invented role policy "+col);}require(!r.next(),"extra role");});
    batchQuery("dg_Batch_RetrieveRoleGrants","select member id, roleid role_id, admin_option\n          from pg_catalog.pg_auth_members order by id, roleid::text",new String[]{"id","role_id","admin_option"},new String[]{"oid","oid","bool"},JdbcProbe::absent);
    batchQuery("dg_Batch_RetrieveTablespaces","select T.oid::bigint as id, T.spcname as name,\n       T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,\n       pg_catalog.pg_tablespace_location(T.oid) /* null */ as location,\n       T.spcoptions /* null */ as options,\n       D.description as comment\nfrom pg_catalog.pg_tablespace T\n  left join pg_catalog.pg_shdescription D on D.objoid = T.oid\n--  where pg_catalog.age(T.xmin) <= #TXAGE",new String[]{"id","name","state_number","owner","location","options","comment"},new String[]{"int8","name","xid","name","text","_text","text"},JdbcProbe::absent);
    batchQuery("dg_Batch_DatabaseTablespaceACL","select T.oid as object_id,\n                 T.spcacl as acl\n          from pg_catalog.pg_tablespace T \n          union all\n          select T.oid as object_id,\n                 T.datacl as acl\n          from pg_catalog.pg_database T ",new String[]{"object_id","acl"},new String[]{"oid","_aclitem"},r->{int count=0;while(r.next()){require(r.getLong(1)>0&&!r.wasNull(),"database oid");require(r.getObject(2)==null&&r.wasNull(),"invented ACL");count++;}require(count==1,"exact database ACL rows");});
    batchQuery("dg_Batch_RetrieveForeignDataWrappers","select fdw.oid as id,\n       fdw.xmin as state_number,\n       fdw.fdwname as name,\n       pr.proname as handler,\n       nspc.nspname as handler_schema,\n       pr2.proname as validator,\n       nspc2.nspname as validator_schema,\n       fdw.fdwoptions as options,\n       pg_catalog.pg_get_userbyid(fdw.fdwowner) as \"owner\"\nfrom pg_catalog.pg_foreign_data_wrapper fdw\n     left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid\n     left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid\n     left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid\n     left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid\n--  where pg_catalog.age(fdw.xmin) <= #TXAGE",new String[]{"id","state_number","name","handler","handler_schema","validator","validator_schema","options","owner"},new String[]{"oid","xid","name","name","name","name","name","_text","name"},JdbcProbe::absent);
    batchQuery("dg_Batch_RetrieveForeignServers","select srv.oid as id,\n       srv.srvfdw as fdw_id,\n       srv.xmin as state_number,\n       srv.srvname as name,\n       srv.srvtype as type,\n       srv.srvversion as version,\n       srv.srvoptions as options,\n       pg_catalog.pg_get_userbyid(srv.srvowner) as \"owner\"\nfrom pg_catalog.pg_foreign_server srv\n--  where pg_catalog.age(srv.xmin) <= #TXAGE",new String[]{"id","fdw_id","state_number","name","type","version","options","owner"},new String[]{"oid","oid","xid","name","text","text","_text","name"},JdbcProbe::absent);
    batchQuery("dg_Batch_RetrieveUserMappings","select umid as id,\n       srvid as server_id,\n       usename as user,\n       umoptions as options\nfrom pg_catalog.pg_user_mappings\norder by server_id",new String[]{"id","server_id","user","options"},new String[]{"oid","oid","name","_text"},JdbcProbe::absent);
    batchQuery("dg_Batch_RetrieveAccessMethods","select A.oid as access_method_id,\n       A.xmin as state_number,\n       A.amname as access_method_name\n       ,\n       A.amhandler::oid as handler_id,\n       pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name,\n       A.amtype as access_method_type\n       \nfrom pg_am A\n     join pg_proc P on A.amhandler::oid = P.oid\n     join pg_namespace N on P.pronamespace = N.oid\n     \n--  where pg_catalog.age(A.xmin) <= #TXAGE",new String[]{"access_method_id","state_number","access_method_name","handler_id","handler_name","access_method_type"},new String[]{"oid","xid","name","oid","text","char"},JdbcProbe::absent);
    batchQuery("dg_Batch_RetrieveExtensions","select E.oid        as id,\n       E.xmin       as state_number,\n       extname      as name,\n       extversion   as version,\n       extnamespace as schema_id,\n       nspname      as schema_name\n       ,\n       array(select unnest\n             from unnest(available_versions)\n             where unnest > extversion) as available_updates\n       \nfrom pg_catalog.pg_extension E\n       join pg_namespace N on E.extnamespace = N.oid\n       left join (select name, array_agg(version) as available_versions\n                  from pg_available_extension_versions()\n                  group by name) V on E.extname = V.name\n       \n--  where pg_catalog.age(E.xmin) <= #TXAGE",new String[]{"id","state_number","name","version","schema_id","schema_name","available_updates"},new String[]{"oid","xid","name","text","oid","name","_text"},JdbcProbe::absent);
    batchQuery("dg_Batch_SchemaForeignACL","select T.oid as object_id,\n                 T.fdwacl as acl\n          from pg_catalog.pg_foreign_data_wrapper T \n          union all\n          select T.oid as object_id,\n                 T.lanacl as acl\n          from pg_catalog.pg_language T \n          union all\n          select T.oid as object_id,\n                 T.nspacl as acl\n          from pg_catalog.pg_namespace T \n          union all\n          select T.oid as object_id,\n                 T.srvacl as acl\n          from pg_catalog.pg_foreign_server T \n          ",new String[]{"object_id","acl"},new String[]{"oid","_aclitem"},r->{Set<Long> ids=new HashSet<>();while(r.next()){long id=r.getLong(1);require(id>0&&!r.wasNull()&&ids.add(id),"schema oid");require(r.getObject(2)==null&&r.wasNull(),"invented ACL");}require(ids.size()==2,"exact authorized schema ACL rows");});
    batchQuery("dg_Batch_ListCasts","select C.oid,\n       C.xmin as state_number,\n       C.castsource as castsource_id,\n       pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name,\n       C.casttarget as casttarget_id,\n       pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name,\n       C.castfunc as castfunc_id,\n       pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name,\n       C.castcontext,\n       C.castmethod\nfrom pg_cast C\n     left outer join pg_proc F on C.castfunc::oid = F.oid\n     left outer join pg_namespace FN on F.pronamespace = FN.oid\n     join pg_type S on C.castsource::oid = S.oid\n     join pg_namespace SN on S.typnamespace = SN.oid\n     join pg_type T on C.casttarget::oid = T.oid\n     join pg_namespace TN on T.typnamespace = TN.oid\n--  where pg_catalog.age(C.xmin) <= #TXAGE",new String[]{"oid","state_number","castsource_id","castsource_name","casttarget_id","casttarget_name","castfunc_id","castfunc_name","castcontext","castmethod"},new String[]{"oid","xid","oid","text","oid","text","oid","text","char","char"},JdbcProbe::absent);
    batchQuery("dg_Batch_RetrieveExtensionMembers","select E.oid   as extension_id,\n       D.objid as member_id\nfrom pg_extension E\n     join pg_depend D on E.oid = D.refobjid and\n                         D.refclassid = 'pg_extension'::regclass::oid\nwhere D.deptype = 'e'\norder by extension_id",new String[]{"extension_id","member_id"},new String[]{"oid","oid"},JdbcProbe::absent);
    noMVCC("dg_StateNumber","select L.transactionid::varchar::bigint as transaction_id\nfrom pg_catalog.pg_locks L\nwhere L.transactionid is not null\norder by pg_catalog.age(L.transactionid) desc\nlimit 1\n;");
    noMVCC("dg_CurrentXid","select case\n  when pg_catalog.pg_is_in_recovery()\n    then null\n  else\n    (pg_catalog.txid_current() % 4294967296)::varchar::bigint\n  end as current_txid\n;");
  }
  static void discovery(){
    discoveryBatch();
    discoveryApp();
    test("dg_DetectionVersion",c->{try(var st=c.prepareStatement("select version()");var r=st.executeQuery()){
      require(r.next(),"missing version");require(r.getString(1).startsWith("GigglesDB "),"truthful implementation identity");require(!r.next(),"extra version row");
    }});
    test("dg_ServerStartupTime",c->{
      String sql="select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time";
      long first;
      try(var st=c.prepareStatement(sql);var r=st.executeQuery()){require(r.next(),"missing startup");first=r.getLong(1);require(!r.wasNull()&&first>0&&first<=System.currentTimeMillis()/1000,"real startup epoch");require(!r.next(),"extra startup row");}
      try(var st=c.createStatement();var r=st.executeQuery(sql)){require(r.next(),"missing repeated startup");require(r.getLong(1)==first&&!r.wasNull(),"stable startup");}
    });
    test("dg_ColumnTypeLookup",c->{try(var st=c.createStatement();var r=st.executeQuery("select xmin from pg_catalog.pg_namespace")){
      require(r.getMetaData().getColumnType(1)==Types.OTHER,"xid JDBC type");require("xid".equals(r.getMetaData().getColumnTypeName(1)),"xid name");require(r.next(),"namespace missing");require(r.getObject(1)==null&&r.wasNull(),"no invented MVCC tuple");
    }});

    test("dg_IsSuperUser",c->{try(var st=c.createStatement();var r=st.executeQuery("select usesuper\nfrom pg_user\nwhere usename = current_user\n;")){
      require(r.next(),"user missing");require(!r.getBoolean("usesuper")&&!r.wasNull(),"application role must not be superuser");require(!r.next(),"extra user");
    }});
    test("dg_ListDatabases",c->{try(var st=c.createStatement();var r=st.executeQuery("select N.oid::bigint as id,\n       datname as name,\n       D.description,\n       datistemplate as is_template,\n       datallowconn as allow_connections,\n       pg_catalog.pg_get_userbyid(N.datdba) as \"owner\"\nfrom pg_catalog.pg_database N\n  left join pg_catalog.pg_shdescription D on N.oid = D.objoid\norder by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end\n;")){
      require(r.next(),"database missing");require(r.getLong("id")>0&&!r.wasNull(),"database oid");require("giggles".equals(r.getString("name")),"database name");require(r.getString("description")==null,"invented database description");require(!r.getBoolean("is_template")&&!r.wasNull(),"template flag");require(r.getBoolean("allow_connections")&&!r.wasNull(),"connections flag");require("giggles".equals(r.getString("owner")),"database owner");require(!r.next(),"extra database");
    }});
    test("dg_ListSchemas",c->{try(var st=c.createStatement();var r=st.executeQuery("select N.oid::bigint as id,\n       N.xmin as state_number,\n       nspname as name,\n       D.description,\n       pg_catalog.pg_get_userbyid(N.nspowner) as \"owner\"\nfrom pg_catalog.pg_namespace N\n  left join pg_catalog.pg_description D on N.oid = D.objoid\norder by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end;")){Set<String> names=new HashSet<>(); while(r.next()){String name=r.getString("name");require(names.add(name),"duplicate schema");require(r.getLong("id")>0&&!r.wasNull(),"schema oid");require(r.getString("state_number")==null&&r.wasNull(),"invented PG MVCC version");require(r.getString("description")==null,"invented schema description");require("giggles".equals(r.getString("owner")),"schema owner");}require(names.equals(Set.of("public","pg_catalog")),"schema names="+names);}});
    test("dg_ListExistentLanguages",c->{try(var st=c.createStatement();var r=st.executeQuery("select oid from pg_catalog.pg_language;")){require(!r.next(),"invented unsupported object");}});
    test("dg_ListLanguages",c->{try(var st=c.createStatement();var r=st.executeQuery("select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted,\n       h.proname as handler, hs.nspname as handlerSchema,\n       i.proname as inline, isc.nspname as inlineSchema,\n       v.proname as validator, vs.nspname as validatorSchema\nfrom pg_catalog.pg_language l\n    left join pg_catalog.pg_proc h on h.oid = lanplcallfoid\n    left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace\n    left join pg_catalog.pg_proc i on i.oid = laninline\n    left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace\n    left join pg_catalog.pg_proc v on v.oid = lanvalidator\n    left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace\norder by lanname;")){require(!r.next(),"invented unsupported object");}});
    test("dg_RetrieveEventTriggers",c->{try(var st=c.createStatement();var r=st.executeQuery("select t.oid as id,\n       t.xmin as state_number,\n       t.evtname as name,\n       t.evtevent as event,\n       t.evtfoid as routine_id,\n       pg_catalog.pg_get_userbyid(t.evtowner) as owner,\n       t.evttags as tags,\n       case when t.evtenabled = 'D' then 1 else 0 end as is_disabled\nfrom pg_catalog.pg_event_trigger t;")){require(!r.next(),"invented unsupported object");}});
    emit("dg_remaining_templates","UNTESTED","retained full PgIntroQueries.sql inventory; remaining sections need independently defined engine facts and DataGrip template parameter expansion, no GUI execution claim");
  }
  public static void main(String[] args) throws Exception {
    port=args[0];profile=args[1];
    // Capture actual driver-generated SQL without enabling parameter/certificate logging.
    var logger=java.util.logging.Logger.getLogger("org.postgresql.core.v3.QueryExecutorImpl");
    logger.setUseParentHandlers(false);logger.setLevel(java.util.logging.Level.FINEST);
    logger.addHandler(new java.util.logging.Handler(){
      public void publish(java.util.logging.LogRecord r){String m=r.getMessage();
        if(m.startsWith(" FE=> Parse(")||m.startsWith(" FE=> SimpleQuery(")){
          String rendered=r.getParameters()==null?m:java.text.MessageFormat.format(m,r.getParameters());
          System.err.println("SQL\t"+activeCase+"\t"+Base64.getEncoder().encodeToString(rendered.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
      }
      public void flush(){} public void close(){}
    });
    String v=org.postgresql.util.DriverInfo.DRIVER_VERSION;System.out.println("VERSION\t"+v);
    if(!v.equals("42.7.3"))throw new IllegalStateException("expected pgJDBC 42.7.3, got "+v);
    if(profile.equals("datagrip-discovery")){discovery();return;}
    test("keepalive",c->{
      for(String sql:List.of("SELECT 1","/* keep-alive */ select 1;")){
        try(var st=c.createStatement();var r=st.executeQuery(sql)){require(r.next(),"missing keepalive row");require(r.getMetaData().getColumnType(1)==Types.INTEGER,"keepalive type");require(r.getInt(1)==1&&!r.wasNull(),"keepalive value");require(!r.next(),"extra keepalive row");}
        try(var st=c.prepareStatement(sql);var r=st.executeQuery()){require(r.next(),"missing prepared keepalive row");require(r.getInt(1)==1&&!r.wasNull(),"prepared keepalive value");require(!r.next(),"extra prepared keepalive row");}
      }
    });
    test("identity",c->{try(var st=c.createStatement();var r=st.executeQuery("SELECT current_database(), current_schema(), current_user, session_user")){
      require(r.next(),"identity row missing");require("giggles".equals(r.getString(1)),"database");require("public".equals(r.getString(2)),"schema");require("giggles".equals(r.getString(3)),"current user");require("giggles".equals(r.getString(4)),"session user");require(!r.next(),"extra identity row");}});
    test("connection_schema",c->require("public".equals(c.getSchema()),"getSchema must be public"));
    test("connection_isolation",c->require(c.getTransactionIsolation()==Connection.TRANSACTION_SERIALIZABLE,"engine isolation must be serializable"));
    test("metadata_catalogs",c->{try(var r=c.getMetaData().getCatalogs()){require(r.next(),"catalog missing");require("giggles".equals(r.getString("TABLE_CAT")),"catalog identity");require(!r.next(),"extra catalog");}});
    test("metadata_schemas",c->{try(var r=c.getMetaData().getSchemas("giggles","public")){require(r.next(),"public schema missing");require("public".equals(r.getString("TABLE_SCHEM")),"schema identity");require(r.getString("TABLE_CATALOG")==null && r.wasNull(),"schema catalog");require(!r.next(),"duplicate/extra schema");}});
    test("metadata_table_filter",c->{try(var r=c.getMetaData().getTables("giggles","public","app",new String[]{"TABLE"})){require(r.next(),"app missing");require("app".equals(r.getString("TABLE_NAME")),"table");require("public".equals(r.getString("TABLE_SCHEM")),"table schema");require(r.getString("TABLE_CAT")==null && r.wasNull(),"table catalog");require("TABLE".equals(r.getString("TABLE_TYPE")),"table type");require(!r.next(),"duplicate app");}try(var r=c.getMetaData().getTables("giggles","public","other",new String[]{"TABLE"})){require(!r.next(),"hidden table disclosed");}try(var r=c.getMetaData().getTables("giggles","public","does_not_exist",null)){require(!r.next(),"invented table");}});
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
    test("metadata_columns",c->{Map<String,Integer> cols=new TreeMap<>();try(var r=c.getMetaData().getColumns(null,null,"app","%")){while(r.next()){require(cols.size()<100,"column bound");String name=r.getString("COLUMN_NAME");require(!cols.containsKey(name),"duplicate column "+name);require(r.getInt("ORDINAL_POSITION")==cols.size()+1,"column order");require("app".equals(r.getString("TABLE_NAME")),"column table");require("public".equals(r.getString("TABLE_SCHEM")),"column schema");require(r.getString("TABLE_CAT")==null && r.wasNull(),"column catalog");require(r.getInt("NULLABLE")==("id".equals(name)?DatabaseMetaData.columnNoNulls:DatabaseMetaData.columnNullable),"column nullability "+name);cols.put(name,r.getInt("DATA_TYPE"));}}
      require(cols.equals(Map.of("id",Types.BIGINT,"body",Types.VARCHAR,"flag",Types.BIT,"small",Types.SMALLINT,"num",Types.INTEGER,"data",Types.BINARY)),"column types="+cols);});
    test("metadata_pk",c->{List<String> keys=new ArrayList<>();try(var r=c.getMetaData().getPrimaryKeys(null,null,"app")){while(r.next()){require(keys.size()<100,"pk bound");require(r.getString("TABLE_CAT")==null && r.wasNull(),"PK catalog");require("public".equals(r.getString("TABLE_SCHEM")),"PK schema");require("app".equals(r.getString("TABLE_NAME")),"PK table");require(r.getShort("KEY_SEQ")==keys.size()+1,"PK sequence");require(r.getString("PK_NAME")!=null&&!r.getString("PK_NAME").isEmpty(),"PK name missing");keys.add(r.getString("COLUMN_NAME"));}}require(keys.equals(List.of("id")),"PK="+keys);});
    test("metadata_types",c->{Set<Integer> types=new HashSet<>();int count=0;try(var r=c.getMetaData().getTypeInfo()){while(r.next()){require(++count<1000,"type bound");types.add(r.getInt("DATA_TYPE"));}}require(types.containsAll(Set.of(Types.BIGINT,Types.VARCHAR,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BINARY)),"type map="+types);});
    test("metadata_type_names",c->{Map<String,Integer> expected=Map.of("int8",Types.BIGINT,"text",Types.VARCHAR,"bool",Types.BIT,"int2",Types.SMALLINT,"int4",Types.INTEGER,"bytea",Types.BINARY);Map<String,Integer> found=new TreeMap<>();int count=0;try(var r=c.getMetaData().getTypeInfo()){while(r.next()){require(++count<1000,"type bound");String n=r.getString("TYPE_NAME");if(expected.containsKey(n)){require(!found.containsKey(n),"duplicate type "+n);found.put(n,r.getInt("DATA_TYPE"));}}}require(found.equals(expected),"named type mapping="+found);});
    test("metadata_hidden",c->{try(var r=c.getMetaData().getColumns(null,null,"other","%")){require(!r.next(),"unauthorized metadata disclosed");}});
    test("repeat_prepare_close",c->{for(int i=0;i<12;i++)empty(c,8000);});
    test("close",c->{c.close();require(c.isClosed(),"close not reflected");require(!c.isValid(1),"closed connection valid");});
    emit("composite_pk","UNTESTED","offline fixture has only one granted table and a single-column PK; no owned admin schema channel");
    emit("multiple_tables","UNTESTED","offline fixture grants app only; hidden other negative control is executed separately");
    emit("schema_fence","UNTESTED","no runner-owned administrative schema-change channel");
    emit("cancel","UNTESTED","no bounded long-running authorized query fixture; authenticated CancelRequest contract requires separate control");
  }
}
