// Strict, deterministic driver observations; never emit server error messages.
package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type obj = map[string]any
type assertion string

func (a assertion) Error() string { return string(a) }
func must(ok bool, reason string) {
	if !ok {
		panic(assertion(reason))
	}
}

var ctx = context.Background()
var prefix, table string

func failure(e error) obj {
	var p *pgconn.PgError
	var state any
	if errors.As(e, &p) {
		state = p.Code
	}
	o := obj{"sqlstate": state, "exception_class": fmt.Sprintf("%T", e)}
	var a assertion
	if errors.As(e, &a) {
		o["reason"] = string(a)
	}
	return o
}
func observe(fn func() any) (o obj) {
	defer func() {
		if p := recover(); p != nil {
			e, ok := p.(error)
			if !ok {
				e = assertion("unexpected_panic")
			}
			o = failure(e)
			o["status"] = "ERR"
		}
	}()
	return obj{"status": "OK", "data": fn()}
}
func check(e error) {
	if e != nil {
		panic(e)
	}
}
func connect() *pgx.Conn {
	cfg, e := pgx.ParseConfig("")
	check(e)
	cfg.TLSConfig = nil
	cfg.RuntimeParams["application_name"] = os.Getenv("PGAPPNAME")
	cfg.ConnectTimeout = 10 * time.Second
	c, e := pgx.ConnectConfig(ctx, cfg)
	check(e)
	return c
}
func canonical(v any) any {
	switch x := v.(type) {
	case []byte:
		return hex.EncodeToString(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case pgtype.Numeric:
		if !x.Valid {
			return nil
		}
		b, e := x.MarshalJSON()
		check(e)
		return string(b)
	case [16]byte:
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16])
	default:
		return v
	}
}

var types = map[uint32]string{16: "bool", 17: "bytea", 20: "int8", 21: "int2", 23: "int4", 25: "text", 700: "float4", 701: "float8", 1043: "varchar", 1184: "timestamptz", 1700: "numeric", 2950: "uuid"}

func query(c *pgx.Conn, sql string, args ...any) obj {
	r, e := c.Query(ctx, sql, args...)
	check(e)
	return collect(r)
}
func collect(r pgx.Rows) obj {
	defer r.Close()
	fs := []any{}
	for _, f := range r.FieldDescriptions() {
		t := types[f.DataTypeOID]
		if t == "" {
			t = "unknown"
		}
		fs = append(fs, obj{"name": f.Name, "type": t})
	}
	rows := [][]any{}
	nulls := [][]bool{}
	for r.Next() {
		v, e := r.Values()
		check(e)
		ns := []bool{}
		for i, x := range v {
			ns = append(ns, x == nil)
			v[i] = canonical(x)
		}
		rows = append(rows, v)
		nulls = append(nulls, ns)
	}
	check(r.Err())
	return obj{"rows": rows, "fields": fs, "nulls": nulls}
}
func expected(c *pgx.Conn, sql string, want [][]any, args ...any) obj {
	r := query(c, sql, args...)
	return checkedResult(r, want)
}
func checkedResult(r obj, want [][]any) obj {
	a, e := json.Marshal(r["rows"])
	check(e)
	b, e := json.Marshal(want)
	check(e)
	must(string(a) == string(b), "rows_mismatch")
	return r
}

// Explicit Parse OIDs preserve the parameter-type probe even when PostgreSQL
// would otherwise resolve an untyped overloaded expression to text.
func typed(c *pgx.Conn, sql string, oids []uint32, values [][]byte, formats []int16, want [][]any) obj {
	r := c.PgConn().ExecParams(ctx, sql, values, oids, formats, nil).Read()
	check(r.Err)
	fields := []any{}
	for _, f := range r.FieldDescriptions {
		t := types[f.DataTypeOID]
		if t == "" {
			t = "unknown"
		}
		fields = append(fields, obj{"name": f.Name, "type": t})
	}
	rows := [][]any{}
	nulls := [][]bool{}
	for _, raw := range r.Rows {
		row := []any{}
		ns := []bool{}
		for i, value := range raw {
			ns = append(ns, value == nil)
			var decoded any
			if value != nil {
				f := r.FieldDescriptions[i]
				t, ok := c.TypeMap().TypeForOID(f.DataTypeOID)
				must(ok, "unknown_result_type")
				var e error
				decoded, e = t.Codec.DecodeValue(c.TypeMap(), f.DataTypeOID, f.Format, value)
				check(e)
			}
			row = append(row, canonical(decoded))
		}
		rows = append(rows, row)
		nulls = append(nulls, ns)
	}
	return checkedResult(obj{"rows": rows, "fields": fields, "nulls": nulls}, want)
}
func exec(c *pgx.Conn, sql string, args ...any) obj {
	t, e := c.Exec(ctx, sql, args...)
	check(e)
	return obj{"row_count": t.RowsAffected()}
}
func insert(c *pgx.Conn, id int) any {
	r := exec(c, "INSERT INTO "+table+" (id,name) VALUES ($1,$2)", id, fmt.Sprintf("tx%d", id))
	must(r["row_count"] == int64(1), "rowcount_mismatch")
	return r
}
func point(c *pgx.Conn, id int, want [][]any, args ...any) obj {
	args = append(args, id)
	r := expected(c, "SELECT id,name,score FROM "+table+" WHERE id=$1", want, args...)
	must(reflect.DeepEqual(r["fields"], []any{obj{"name": "id", "type": "int4"}, obj{"name": "name", "type": "text"}, obj{"name": "score", "type": "int8"}}), "point_fields_mismatch")
	return r
}
func step(o obj, k string, fn func() any) { o[k] = observe(fn) }
func state(c *pgx.Conn, sql, want string) any {
	_, e := c.Exec(ctx, sql)
	must(e != nil, "expected_error_missing")
	var p *pgconn.PgError
	if !errors.As(e, &p) || p.Code != want {
		panic(e)
	}
	return obj{"sqlstate": p.Code}
}
func transaction(c *pgx.Conn, commit bool) any {
	o := obj{}
	id := 11
	end := "ROLLBACK"
	if commit {
		id = 10
		end = "COMMIT"
	}
	step(o, "begin", func() any { return exec(c, "BEGIN") })
	step(o, "insert", func() any { return insert(c, id) })
	step(o, "end", func() any { return exec(c, end) })
	step(o, "visibility", func() any {
		d := connect()
		defer d.Close(ctx)
		want := [][]any{}
		if commit {
			want = [][]any{{10, "tx10", nil}}
		}
		return point(d, id, want)
	})
	return o
}
func roundtrip(c *pgx.Conn) any {
	specs := []struct {
		name, decl string
		value      any
		want       any
	}{{"boolean", "boolean", true, true}, {"bytea", "bytea", []byte{0, 1, 255}, "0001ff"}, {"numeric", "numeric(10,2)", pgtype.Numeric{Int: big.NewInt(1230), Exp: -2, Valid: true}, "12.30"}, {"smallint", "smallint", int16(-12), -12}, {"double", "double precision", 1.25, 1.25}, {"varchar", "varchar(20)", "text", "text"}, {"timestamptz", "timestamptz", time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), "2024-01-02T03:04:05Z"}, {"uuid", "uuid", pgtype.UUID{Bytes: [16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}, Valid: true}, "123e4567-e89b-12d3-a456-426614174000"}}
	out := obj{}
	for _, s := range specs {
		t := prefix + "_t_" + s.name
		o := obj{}
		step(o, "create", func() any { return exec(c, "CREATE TABLE "+t+" (id integer PRIMARY KEY,v "+s.decl+")") })
		if o["create"].(obj)["status"] == "OK" {
			for id := 1; id <= 2; id++ {
				var value, want any
				if id == 1 {
					value = s.value
					want = s.want
				}
				step(o, fmt.Sprintf("insert_%d", id), func() any {
					r := exec(c, "INSERT INTO "+t+" (id,v) VALUES ($1,$2)", id, value)
					must(r["row_count"] == int64(1), "rowcount_mismatch")
					return r
				})
				step(o, fmt.Sprintf("point_%d", id), func() any { return expected(c, "SELECT id,v FROM "+t+" WHERE id=$1", [][]any{{id, want}}, id) })
			}
		}
		status := "OK"
		for _, v := range o {
			if v.(obj)["status"] != "OK" {
				status = "ERR"
			}
		}
		out[s.name] = obj{"status": status, "steps": o}
	}
	return out
}
func main() {
	prefix = os.Getenv("R0_PREFIX")
	if prefix == "" {
		prefix = "r0d_pgx"
	}
	must(regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}$`).MatchString(prefix), "unsafe_prefix")
	table = prefix + "_items"
	bi, ok := debug.ReadBuildInfo()
	must(ok, "build_info_missing")
	version := ""
	for _, d := range bi.Deps {
		if d.Path == "github.com/jackc/pgx/v5" {
			version = d.Version
		}
	}
	must(version == "v5.7.6", "driver_version_mismatch")
	fmt.Println("VERSION\tpgx\t5.7.6")
	cases := []struct {
		id string
		fn func(*pgx.Conn) any
	}{
		{"connect", func(c *pgx.Conn) any {
			o := obj{}
			step(o, "identity", func() any { return query(c, "SELECT current_user,current_database()") })
			step(o, "parameter_status", func() any {
				p := obj{}
				for _, k := range []string{"server_encoding", "client_encoding", "integer_datetimes"} {
					v := c.PgConn().ParameterStatus(k)
					must(v != "", "parameter_status_missing")
					p[k] = v
				}
				return p
			})
			return o
		}},
		{"keepalive", func(c *pgx.Conn) any {
			return obj{"simple": observe(func() any { return expected(c, "SELECT 1", [][]any{{1}}, pgx.QueryExecModeSimpleProtocol) }), "prepared": observe(func() any { return expected(c, "SELECT $1::integer", [][]any{{1}}, 1) })}
		}},
		{"ddl_create", func(c *pgx.Conn) any {
			return exec(c, "CREATE TABLE "+table+" (id integer PRIMARY KEY,name text,score bigint)")
		}},
		{"prepared_insert", func(c *pgx.Conn) any {
			o := obj{}
			for i, n := range []string{"a", "b", "c"} {
				id := i + 1
				var score any = int64(20)
				if id == 2 {
					score = nil
				}
				step(o, fmt.Sprintf("row_%d", id), func() any {
					r := exec(c, "INSERT INTO "+table+" (id,name,score) VALUES ($1,$2,$3)", id, n, score)
					must(r["row_count"] == int64(1), "rowcount_mismatch")
					return r
				})
			}
			return o
		}},
		{"prepared_point_select", func(c *pgx.Conn) any {
			return obj{"row": observe(func() any { return point(c, 1, [][]any{{1, "a", 20}}) }), "null_row": observe(func() any { return point(c, 2, [][]any{{2, "b", nil}}) })}
		}},
		{"prepared_repeat", func(c *pgx.Conn) any {
			o := []any{}
			for i := 0; i < 7; i++ {
				o = append(o, observe(func() any { return point(c, 2, [][]any{{2, "b", nil}}) }))
			}
			return obj{"executions": o}
		}},
		{"select_ordered", func(c *pgx.Conn) any {
			return expected(c, "SELECT id,name,score FROM "+table+" ORDER BY id", [][]any{{1, "a", 20}, {2, "b", nil}, {3, "c", 20}})
		}},
		{"update_delete_params", func(c *pgx.Conn) any {
			o := obj{}
			step(o, "update", func() any {
				r := exec(c, "UPDATE "+table+" SET score=score+$1 WHERE id=$2", 5, 1)
				must(r["row_count"] == int64(1), "rowcount_mismatch")
				return r
			})
			step(o, "delete", func() any {
				r := exec(c, "DELETE FROM "+table+" WHERE id=$1", 3)
				must(r["row_count"] == int64(1), "rowcount_mismatch")
				return r
			})
			step(o, "point", func() any {
				if o["update"].(obj)["status"] == "OK" {
					return point(c, 1, [][]any{{1, "a", 25}})
				}
				return query(c, "SELECT id,name,score FROM "+table+" WHERE id=$1", 1)
			})
			return o
		}},
		{"tx_commit", func(c *pgx.Conn) any { return transaction(c, true) }}, {"tx_rollback", func(c *pgx.Conn) any { return transaction(c, false) }},
		{"tx_error_recovery", func(c *pgx.Conn) any {
			o := obj{}
			step(o, "begin", func() any { return exec(c, "BEGIN") })
			step(o, "duplicate", func() any { return state(c, "INSERT INTO "+table+" (id) VALUES (1)", "23505") })
			step(o, "aborted", func() any { return state(c, "SELECT 1", "25P02") })
			step(o, "rollback", func() any { return exec(c, "ROLLBACK") })
			step(o, "recovered", func() any { return expected(c, "SELECT 1", [][]any{{1}}) })
			return o
		}},
		{"savepoint", func(c *pgx.Conn) any {
			o := obj{}
			step(o, "begin", func() any { return exec(c, "BEGIN") })
			step(o, "insert_a", func() any { return insert(c, 20) })
			step(o, "savepoint", func() any { return exec(c, "SAVEPOINT probe_sp") })
			step(o, "insert_b", func() any { return insert(c, 21) })
			for _, s := range []struct{ k, sql string }{{"rollback_to", "ROLLBACK TO SAVEPOINT probe_sp"}, {"release", "RELEASE SAVEPOINT probe_sp"}, {"commit", "COMMIT"}} {
				step(o, s.k, func() any { return exec(c, s.sql) })
			}
			step(o, "point_a", func() any { return point(c, 20, [][]any{{20, "tx20", nil}}) })
			step(o, "point_b", func() any { return point(c, 21, [][]any{}) })
			return o
		}},
		{"types_roundtrip", roundtrip},
		{"param_bytea_position", func(c *pgx.Conn) any {
			o := obj{}
			for _, s := range []struct{ k, sql string }{{"cast", "SELECT position($1::bytea IN $2::bytea)"}, {"uncast", "SELECT position($1 IN $2)"}} {
				step(o, s.k, func() any {
					return typed(c, s.sql, []uint32{17, 17}, [][]byte{{2}, {1, 2, 3}}, []int16{1, 1}, [][]any{{2}})
				})
			}
			return o
		}},
		{"param_no_from_aggregate", func(c *pgx.Conn) any {
			return obj{"count": observe(func() any { return expected(c, "SELECT count(*)", [][]any{{1}}) }), "max": observe(func() any {
				return typed(c, "SELECT max($1)", []uint32{23}, [][]byte{[]byte("7")}, []int16{0}, [][]any{{7}})
			})}
		}},
		{"exec_mode_simple", func(c *pgx.Conn) any { return point(c, 2, [][]any{{2, "b", nil}}, pgx.QueryExecModeSimpleProtocol) }},
		{"batch", func(c *pgx.Conn) any {
			o := obj{}
			b := &pgx.Batch{}
			for _, id := range []int{40, 41} {
				b.Queue("INSERT INTO "+table+" (id,name) VALUES ($1,$2)", id, fmt.Sprintf("batch%d", id))
			}
			b.Queue("SELECT id,name,score FROM "+table+" WHERE id=$1", 40)
			r := c.SendBatch(ctx, b)
			for _, id := range []int{40, 41} {
				step(o, fmt.Sprintf("insert_%d", id), func() any {
					t, e := r.Exec()
					check(e)
					must(t.RowsAffected() == 1, "rowcount_mismatch")
					return obj{"row_count": t.RowsAffected()}
				})
			}
			step(o, "batch_point", func() any {
				rows, e := r.Query()
				check(e)
				result := collect(rows)
				want := obj{"rows": [][]any{{int32(40), "batch40", nil}}, "fields": []any{obj{"name": "id", "type": "int4"}, obj{"name": "name", "type": "text"}, obj{"name": "score", "type": "int8"}}, "nulls": [][]bool{{false, false, true}}}
				must(reflect.DeepEqual(result, want), "batch_point_mismatch")
				return result
			})
			step(o, "close", func() any { check(r.Close()); return true })
			for _, id := range []int{40, 41} {
				step(o, fmt.Sprintf("point_%d", id), func() any { return point(c, id, [][]any{{id, fmt.Sprintf("batch%d", id), nil}}) })
			}
			return o
		}},
		{"copy_from", func(c *pgx.Conn) any {
			o := obj{}
			step(o, "copy", func() any {
				n, e := c.CopyFrom(ctx, pgx.Identifier{table}, []string{"id", "name", "score"}, pgx.CopyFromRows([][]any{{30, "copy30", int64(30)}, {31, "copy31", nil}}))
				check(e)
				must(n == 2, "copy_count_mismatch")
				return obj{"rows_written": n}
			})
			step(o, "point_30", func() any { return point(c, 30, [][]any{{30, "copy30", 30}}) })
			step(o, "point_31", func() any { return point(c, 31, [][]any{{31, "copy31", nil}}) })
			return o
		}},
		{"field_descriptions", func(c *pgx.Conn) any { return point(c, 2, [][]any{{2, "b", nil}}) }},
	}
	for _, tc := range cases {
		o := observe(func() any { c := connect(); defer c.Close(ctx); return tc.fn(c) })
		status := o["status"]
		var payload any = o["data"]
		if status == "ERR" {
			delete(o, "status")
			payload = o
		}
		b, e := json.Marshal(payload)
		check(e)
		fmt.Printf("CASE\t%s\t%s\t%s\n", tc.id, status, base64.StdEncoding.EncodeToString(b))
	}
	fmt.Printf("DONE\t%d\n", len(cases))
}
