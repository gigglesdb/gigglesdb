#!/usr/bin/env python3
"""Live JSON-path checks; print fixed outcomes only, never values or diagnostics."""
import json
import logging
import os
import sys

ORIGINAL = {"status": "sent", "recipients": [{"email": "first@example.test", "phone": "fixture-phone"}, {"email": "second@example.test", "phone": "fixture-phone"}], "processor_response": "fixture-processor"}
CHANGED = {"status": {"unapproved": "fixture-hidden"}, "recipients": [{"email": "first@example.test", "phone": "fixture-phone"}, 7, None, "fixture-hidden", {}], "new_top": "fixture-top"}

def main():
    logging.disable(logging.CRITICAL)
    passed = failed = 0
    def step(label, action):
        nonlocal passed, failed
        try:
            ok = bool(action())
        except Exception:
            ok = False
        print(("PASS " if ok else "FAIL ") + label, flush=True)
        passed += int(ok)
        failed += int(not ok)
        if not ok:
            raise RuntimeError("live check failed")
    try:
        import psycopg
        schema = os.environ["NUNYA_TEST_JSON_SCHEMA"]
        with psycopg.connect(os.environ["NUNYA_TEST_JSON_PG_ADMIN"], autocommit=True) as admin:
            def update(doc):
                from psycopg import sql
                admin.execute(sql.SQL("UPDATE {}.events SET doc_json=%s::json, doc_jsonb=%s::jsonb WHERE id=1").format(sql.Identifier(schema)), (json.dumps(doc), json.dumps(doc)))
            for binary in (False, True):
                label = "psycopg-" + ("binary" if binary else "text")
                show = os.environ.get("NUNYA_TEST_JSON_OPERATORS") == "show"
                update({"payment":{"card":"fixture-hidden","amount":42},"new_top":"fixture-top","items":{"0":{"card":"fixture-hidden"}}} if show else ORIGINAL)
                with psycopg.connect(autocommit=True) as conn:
                    def read():
                        with conn.cursor(binary=binary) as cur:
                            cur.execute("SELECT doc_json, doc_jsonb FROM events WHERE id = %s", (1,), prepare=True)
                            return cur.fetchone()
                    if show:
                        with conn.cursor(binary=binary) as cur:
                            def query(sql):
                                cur.execute(sql, prepare=True)
                                return cur.fetchall()
                            for col in ("doc_json", "doc_jsonb"):
                                whole = query(f"SELECT {col} FROM events")[0][0]
                                step(label+" show-parent", lambda: query(f"SELECT {col}->'payment' FROM events")[0][0] == whole["payment"])
                                step(label+" show-new-key", lambda: query(f"SELECT {col}->>'new_top' FROM events")[0][0] == "fixture-top")
                                step(label+" show-masked", lambda: query(f"SELECT {col} #> '{{payment,card}}' FROM events")[0][0] is None)
                                step(label+" index-container", lambda: query(f"SELECT {col} #> '{{items,0,card}}' FROM events")[0][0] is None)
                                def refused():
                                    try: query(f"SELECT id FROM events WHERE {col} #> '{{payment,card}}' IS NULL")
                                    except psycopg.Error as e: return e.sqlstate == "NY005"
                                    return False
                                step(label+" show-refusal", refused)
                        continue
                    if os.environ.get("NUNYA_TEST_JSON_OPERATORS") == "1":
                        with conn.cursor(binary=binary) as cur:
                            def query(sql, params=None):
                                cur.execute(sql, params, prepare=True)
                                return cur.fetchall()
                            for col in ("doc_json", "doc_jsonb"):
                                whole = query(f"SELECT {col} FROM events")[0][0]
                                step(label+" masked", lambda: query(f"SELECT {col}->'processor_response' FROM events")[0][0] is None)
                                step(label+" parent", lambda: query(f"SELECT {col}->'recipients' FROM events")[0][0] == whole["recipients"])
                                token = query(f"SELECT {col} #>> '{{recipients,0,email}}' FROM events")[0][0]
                                step(label+" token", lambda: token == whole["recipients"][0]["email"])
                                step(label+" show-equality", lambda: len(query(f"SELECT id FROM events WHERE {col}->>'status' = 'sent'")) == 1)
                                step(label+" show-null", lambda: not query(f"SELECT id FROM events WHERE {col}->>'status' IS NULL"))
                                step(label+" token-param", lambda: len(query(f"SELECT id FROM events WHERE {col} #>> '{{recipients,0,email}}' = %s", (token,))) == 1)
                                step(label+" token-literal", lambda: len(query(f"SELECT id FROM events WHERE {col} #>> '{{recipients,0,email}}' = '{token}'")) == 1)
                                step(label+" join", lambda: len(query(f"SELECT e.id FROM events e JOIN customers c ON e.{col} #>> '{{recipients,0,email}}' = c.email")) == 1)
                                for sql in (f"SELECT id FROM events WHERE {col}->>'processor_response' IS NULL", f"SELECT id FROM events WHERE {col}->>'processor_response' = 'x'", f"SELECT {col}->id FROM events", f"SELECT {col} @> '{{}}' FROM events", f"SELECT {col} ? 'status' FROM events", f"SELECT jsonb_path_query(doc_jsonb, '$.status') FROM events", f"UPDATE events SET id=id WHERE {col}->>'status'='sent'"):
                                    def refused():
                                        try: query(sql)
                                        except psycopg.Error as e: return e.sqlstate == "NY005"
                                        return False
                                    step(label+" refusal-NY005", refused)
                            update(CHANGED)
                            step(label+" shape-projection", lambda: query("SELECT doc_jsonb->>'status' FROM events")[0][0] is None)
                            step(label+" shape-predicate", lambda: not query("SELECT id FROM events WHERE doc_jsonb->>'status' = %s", (json.dumps(CHANGED["status"]),)))
                            update(ORIGINAL)
                        continue
                    a, b = read()
                    step(label + " token-and-columns", lambda: a == b and a["status"] == "sent" and len(a["recipients"]) == 2 and all(r["email"].startswith("tok_email_") for r in a["recipients"]) and not any(k in json.dumps(a) for k in ("phone", "processor_response", "example.test")))
                    update(CHANGED)
                    c, d = read()
                    step(label + " shape-change", lambda: c == d and "status" not in c and "new_top" not in c and len(c["recipients"]) == 5 and c["recipients"][1:4] == [None]*3 and c["recipients"][4] == {} and c["recipients"][0]["email"] == a["recipients"][0]["email"])
                    update({"status": None, "recipients": [{}]})
                    e, f = read()
                    step(label + " null-missing", lambda: e == f and e["status"] is None and e["recipients"] == [{}])
                    update(ORIGINAL)
                with psycopg.connect(autocommit=True) as conn, conn.cursor(binary=binary) as cur:
                    cur.execute("SELECT doc_json, doc_jsonb FROM events WHERE id = %s", (1,), prepare=True)
                    step(label + " reconnect-stable", lambda: cur.fetchone() == (a,b))
            update(ORIGINAL)
    except Exception:
        if not failed:
            failed += 1
            print("FAIL psycopg-setup-or-update", flush=True)
    print(f"SUMMARY psycopg passed={passed} failed={failed}", flush=True)
    return int(failed > 0)

if __name__ == "__main__":
    sys.exit(main())