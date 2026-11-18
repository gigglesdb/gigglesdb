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
                update(ORIGINAL)
                with psycopg.connect(autocommit=True) as conn:
                    def read():
                        with conn.cursor(binary=binary) as cur:
                            cur.execute("SELECT doc_json, doc_jsonb FROM events WHERE id = %s", (1,), prepare=True)
                            return cur.fetchone()
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