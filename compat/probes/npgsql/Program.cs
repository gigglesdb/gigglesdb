using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using NpgsqlTypes;

class ProbeAssertion(string reason) : Exception { public string Reason = reason; }
class Probe {
static string Prefix=Environment.GetEnvironmentVariable("R0_PREFIX")??"r0d_npgsql";
static string Table=>Prefix+"_items";
static void Require(bool ok,string reason="wrong_value") { if(!ok) throw new ProbeAssertion(reason); }
static object? Canon(object? v)=>v switch { null=>null,DBNull=>null,byte[] b=>Convert.ToHexString(b).ToLowerInvariant(),long n=>n.ToString(CultureInfo.InvariantCulture),decimal n=>n.ToString("0.00",CultureInfo.InvariantCulture),DateTime d=>d.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ",CultureInfo.InvariantCulture),Guid g=>g.ToString(),_=>v };
static Dictionary<string,object?> Error(Exception e,string? reason=null)=>new(){["sqlstate"]=e is PostgresException p?p.SqlState:null,["exception_class"]=e is ProbeAssertion?"ProbeAssertion":e.GetType().FullName,["reason"]=reason??(e is ProbeAssertion a?a.Reason:"driver_error")};
static Dictionary<string,object?> Obs(Func<object?> f) {try{return new(){["status"]="OK",["data"]=f()};}catch(Exception e){var r=Error(e);r["status"]="ERR";return r;}}
static NpgsqlConnection Open()
{
    var b = new NpgsqlConnectionStringBuilder
    {
        Host = Environment.GetEnvironmentVariable("PGHOST") ?? "localhost",
        Port = int.Parse(Environment.GetEnvironmentVariable("PGPORT") ?? "5432"),
        Username = Environment.GetEnvironmentVariable("PGUSER") ?? "postgres",
        Database = Environment.GetEnvironmentVariable("PGDATABASE") ?? "postgres",
        Password = Environment.GetEnvironmentVariable("PGPASSWORD") ?? "",
        ApplicationName = Environment.GetEnvironmentVariable("PGAPPNAME") ?? "",
        SslMode = SslMode.Disable,
        Timeout = 10,
        CommandTimeout = 30,
        Pooling = false,
        MaxAutoPrepare = 10,
        AutoPrepareMinUsages = 5
    };
    if (Environment.GetEnvironmentVariable("R0_NPGSQL_PROFILE") == "notypes")
        b["Server Compatibility Mode"] = "NoTypeLoading";
    var c = new NpgsqlConnection(b.ConnectionString);
    try { c.Open(); return c; }
    catch { c.Dispose(); throw; }
}
static NpgsqlCommand Cmd(NpgsqlConnection c,string sql,params object?[] args){var q=new NpgsqlCommand(sql,c);foreach(var a in args)q.Parameters.Add(new NpgsqlParameter{Value=a??DBNull.Value});return q;}
static object Exec(NpgsqlConnection c,string sql,params object?[] args){using var q=Cmd(c,sql,args);return new {row_count=q.ExecuteNonQuery()};}
static object One(NpgsqlConnection c,string sql,params object?[] args){using var q=Cmd(c,sql,args);Require(q.ExecuteNonQuery()==1,"wrong_row_count");return new {row_count=1};}
static readonly Dictionary<uint, string> TypeNames = new()
{
    [16] = "bool", [17] = "bytea", [20] = "int8", [21] = "int2",
    [23] = "int4", [25] = "text", [700] = "float4", [701] = "float8",
    [1043] = "varchar", [1184] = "timestamptz", [1700] = "numeric", [2950] = "uuid"
};
static string TypeName(uint oid) => TypeNames.GetValueOrDefault(oid, "unknown");
static object Rows(NpgsqlCommand q,object?[][]? expected=null,bool wasNull=false){using var r=q.ExecuteReader();var fields=Enumerable.Range(0,r.FieldCount).Select(i=>new {name=r.GetName(i),type=TypeName(r.GetPostgresType(i).OID)}).ToArray();var rows=new List<object?[]>();while(r.Read())rows.Add(Enumerable.Range(0,r.FieldCount).Select(i=>Canon(r.GetValue(i))).ToArray());if(expected!=null)Require(JsonSerializer.Serialize(rows)==JsonSerializer.Serialize(expected),"rows_mismatch");return wasNull?new {rows,fields,was_null=rows.Count==1&&rows[0][0]==null}:(object)new {rows,fields};}
static object Query(NpgsqlConnection c,string sql,object?[][]? expected=null,params object?[] args){using var q=Cmd(c,sql,args);return Rows(q,expected);}
static object Point(NpgsqlConnection c,int id,object?[][]? expected=null)=>Query(c,$"SELECT id,name,score FROM {Table} WHERE id=$1",expected,id);
static object Scalar(NpgsqlConnection c,string sql,object? value,params object?[] args)=>Query(c,sql,new[]{new[]{Canon(value)}},args);
static object Insert(NpgsqlConnection c,int id)=>One(c,$"INSERT INTO {Table} (id,name) VALUES ($1,$2)",id,"tx"+id);
static object Expected(NpgsqlConnection c,string sql,string state){try{Exec(c,sql);}catch(PostgresException e){Require(e.SqlState==state,"unexpected_sqlstate");return new{sqlstate=e.SqlState};}throw new ProbeAssertion("expected_error_missing");}
static object Transaction(NpgsqlConnection c,bool commit){var o=new Dictionary<string,object?>();o["begin"]=Obs(()=>Exec(c,"BEGIN"));o["insert"]=Obs(()=>Insert(c,commit?10:11));o[commit?"commit":"rollback"]=Obs(()=>Exec(c,commit?"COMMIT":"ROLLBACK"));o["visibility"]=Obs(()=>{using var d=Open();return Point(d,commit?10:11,commit?new[]{new object?[]{10,"tx10",null}}:Array.Empty<object?[]>());});return o;}
static object Types(NpgsqlConnection c){var o=new Dictionary<string,object?>();(string,string,object)[] specs=[("boolean","boolean",true),("bytea","bytea",new byte[]{0,1,255}),("numeric","numeric(10,2)",12.30m),("smallint","smallint",(short)123),("double","double precision",1.25),("varchar","varchar(20)","hello"),("timestamptz","timestamptz",new DateTime(2020,1,2,3,4,5,DateTimeKind.Utc)),("uuid","uuid",Guid.Parse("12345678-1234-5678-1234-567812345678"))];foreach(var (name,decl,value) in specs){var t=Prefix+"_t_"+name;var s=new Dictionary<string,object?>();s["create"]=Obs(()=>Exec(c,$"CREATE TABLE {t} (id integer PRIMARY KEY,v {decl})"));foreach(var id in new[]{1,2}){var v=id==1?value:null;s["insert_"+id]=Obs(()=>One(c,$"INSERT INTO {t} (id,v) VALUES ($1,$2)",id,v));s["point_"+id]=Obs(()=>{using var q=Cmd(c,$"SELECT v FROM {t} WHERE id=$1",id);return Rows(q,new[]{new[]{Canon(v)}},true);});}o[name]=new {status=s.Values.All(v=>((Dictionary<string,object?>)v!)["status"] as string=="OK")?"OK":"ERR",steps=s};}return o;}
static object Schema(NpgsqlConnection c,bool columns){using var d=c.GetSchema(columns?"Columns":"Tables",columns?new string?[]{null,"public",Table,null}:new string?[]{null,"public",Table,"BASE TABLE"});var names=columns?new[]{"column_name","data_type","is_nullable","ordinal_position"}:new[]{"table_schema","table_name","table_type"};var rows=d.Rows.Cast<DataRow>().Select(r=>names.Select(n=>Canon(r[n])).ToArray()).ToList();if(columns){rows=rows.OrderBy(r=>Convert.ToInt32(r[3])).ToList();Require(rows.Count==3,"schema_columns_missing");Require(rows.Select(r=>r[0]?.ToString()).SequenceEqual(new[]{"id","name","score"}),"schema_column_names");Require(rows.Select(r=>r[1]?.ToString()).SequenceEqual(new[]{"integer","text","bigint"}),"schema_column_types");Require(rows.Select(r=>r[2]?.ToString()).SequenceEqual(new[]{"NO","YES","YES"}),"schema_nullability");Require(rows.Select(r=>Convert.ToInt32(r[3])).SequenceEqual(new[]{1,2,3}),"schema_ordinals");}else Require(rows.Count==1&&rows[0][0]?.ToString()=="public"&&rows[0][1]?.ToString()==Table&&rows[0][2]?.ToString()=="BASE TABLE","schema_table_mismatch");return new {columns=names,rows};}
static List<(string,Func<NpgsqlConnection,object?>)> Cases()=>new(){
("connect",c=>new{identity=Obs(()=>{using var q=Cmd(c,"SELECT current_user,current_database()");using var r=q.ExecuteReader();Require(r.Read(),"missing_identity");Require(r.GetString(0)==(Environment.GetEnvironmentVariable("PGUSER")??"postgres")&&r.GetString(1)==(Environment.GetEnvironmentVariable("PGDATABASE")??"postgres"),"identity_mismatch");return new{user=r.GetString(0),database=r.GetString(1)};}),connection=Obs(()=>Scalar(c,"SELECT 1",1))}),
("keepalive",c=>new{simple=Obs(()=>Scalar(c,"SELECT 1",1)),parameter=Obs(()=>Scalar(c,"SELECT $1::integer",1,1))}),
("ddl_create",c=>Exec(c,$"CREATE TABLE {Table} (id integer PRIMARY KEY,name text,score bigint)")),
("prepared_insert",c=>new[]{(1,"a",(object?)20L),(2,"b",null),(3,"c",(object?)20L)}.ToDictionary(x=>"row_"+x.Item1,x=>Obs(()=>One(c,$"INSERT INTO {Table} (id,name,score) VALUES ($1,$2,$3)",x.Item1,x.Item2,x.Item3)))),
("prepared_point_select",c=>new{row_1=Obs(()=>Point(c,1,new[]{new object?[]{1,"a","20"}})),row_2=Obs(()=>Point(c,2,new[]{new object?[]{2,"b",null}}))}),
("prepared_repeat",c=>{using var q=Cmd(c,$"SELECT id,name,score FROM {Table} WHERE id=$1",1);return new{executions=Enumerable.Range(0,7).Select(_=>Obs(()=>Rows(q,new[]{new object?[]{1,"a","20"}}))).ToArray()};}),
("select_ordered",c=>Query(c,$"SELECT id,name,score FROM {Table} ORDER BY id",new[]{new object?[]{1,"a","20"},new object?[]{2,"b",null},new object?[]{3,"c","20"}})),
("update_delete_params",c=>{var u=Obs(()=>One(c,$"UPDATE {Table} SET score=score+$1 WHERE id=$2",5,1));return new{update=u,delete=Obs(()=>One(c,$"DELETE FROM {Table} WHERE id=$1",3)),point=Obs(()=>Point(c,1,u["status"] as string=="OK"?new[]{new object?[]{1,"a","25"}}:null))};}),
("tx_commit",c=>Transaction(c,true)),("tx_rollback",c=>Transaction(c,false)),
("tx_error_recovery",c=>new{begin=Obs(()=>Exec(c,"BEGIN")),duplicate=Obs(()=>Expected(c,$"INSERT INTO {Table} (id) VALUES (1)","23505")),aborted=Obs(()=>Expected(c,"SELECT 1","25P02")),rollback=Obs(()=>Exec(c,"ROLLBACK")),recovered=Obs(()=>Scalar(c,"SELECT 1",1))}),
("savepoint",c=>new{begin=Obs(()=>Exec(c,"BEGIN")),insert20=Obs(()=>Insert(c,20)),savepoint=Obs(()=>Exec(c,"SAVEPOINT r0_sp")),insert21=Obs(()=>Insert(c,21)),rollback_to=Obs(()=>Exec(c,"ROLLBACK TO SAVEPOINT r0_sp")),release=Obs(()=>Exec(c,"RELEASE SAVEPOINT r0_sp")),commit=Obs(()=>Exec(c,"COMMIT")),point20=Obs(()=>Point(c,20,new[]{new object?[]{20,"tx20",null}})),point21=Obs(()=>Point(c,21,Array.Empty<object?[]>()))}),
("types_roundtrip",Types),
("param_bytea_position",c=>new{cast=Obs(()=>Scalar(c,"SELECT position($1::bytea IN $2::bytea)",2,new byte[]{98,99},new byte[]{97,98,99,100})),uncast=Obs(()=>Scalar(c,"SELECT position($1 IN $2)",2,new byte[]{98,99},new byte[]{97,98,99,100}))}),
("param_no_from_aggregate",c=>new{count=Obs(()=>Scalar(c,"SELECT count(*)",1L)),max=Obs(()=>Scalar(c,"SELECT max($1)",7,7))}),
("get_schema_tables",c=>Schema(c,false)),("get_schema_columns",c=>Schema(c,true)),
("prepare_explicit",c=>{using var q=Cmd(c,$"SELECT id,name,score FROM {Table} WHERE id=$1",2);var prepare=Obs(()=>{q.Prepare();Require(q.IsPrepared,"not_prepared");return new{prepared=q.IsPrepared};});return new{prepare,executions=Enumerable.Range(0,3).Select(_=>Obs(()=>Rows(q,new[]{new object?[]{2,"b",null}}))).ToArray()};}),
("binary_import",c=>new{copy=Obs(()=>{using var w=c.BeginBinaryImport($"COPY {Table} (id,name,score) FROM STDIN (FORMAT BINARY)");w.StartRow();w.Write(30,NpgsqlDbType.Integer);w.Write("copy30",NpgsqlDbType.Text);w.Write(30L,NpgsqlDbType.Bigint);w.StartRow();w.Write(31,NpgsqlDbType.Integer);w.Write("copy31",NpgsqlDbType.Text);w.WriteNull();var count=w.Complete();Require(count==2,"copy_row_count");return new{rows_written=count};}),point30=Obs(()=>Point(c,30,new[]{new object?[]{30,"copy30","30"}})),point31=Obs(()=>Point(c,31,new[]{new object?[]{31,"copy31",null}}))})};
static int Main(){try{Require(Regex.IsMatch(Prefix,"^[a-z][a-z0-9_]{0,47}$"),"unsafe_prefix");var a=typeof(NpgsqlConnection).Assembly;var info=a.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;Require(info?.Split('+')[0]=="9.0.4","driver_version_mismatch");Require(a.GetName().Version?.Major==9,"assembly_version_mismatch");Console.WriteLine("VERSION\tnpgsql\t9.0.4");var cases=Cases();foreach(var (id,f) in cases){Dictionary<string,object?> o;NpgsqlConnection? c=null;try{c=Open();o=Obs(()=>f(c));}catch(Exception e){o=Error(e,"connect_failed");o["status"]="ERR";}finally{c?.Dispose();}var status=o["status"];object? data;if(status as string=="OK")data=o["data"];else{o.Remove("status");data=o;}Console.WriteLine($"CASE\t{id}\t{status}\t{Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data)))}");}Console.WriteLine($"DONE\t{cases.Count}");return 0;}catch(Exception e){Console.Error.WriteLine(JsonSerializer.Serialize(Error(e)));return 1;}}
}



