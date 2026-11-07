import copy
import gzip
import json
import os
import unittest
from types import SimpleNamespace
from conformance import Reference, decode_oracle, oracle_query, parameter_literal

from conformance import IMAGE, ROOT, compare, coverage, digest, validate_cases, validate_inventory, validate_pin


def inventory():
    return {"server_version_num": 180000,
            "routines": [{"identity": "pg_catalog.abs(bigint)/f"},
                         {"identity": "pg_catalog.abs(integer)/f"}],
            **{k: [{}] for k in ("types", "casts", "operators", "aggregates", "namespaces", "extensions")}}


def case():
    return {"id": "abs-normal", "routines": ["pg_catalog.abs(bigint)/f"],
            "kind": "normal", "setup": [], "query": "SELECT abs(-1::bigint) AS v",
            "expected": {"status": "ok", "columns": [{"name": "v", "type": "bigint"}], "rows": [[1]]}}


class CoverageTests(unittest.TestCase):
    def test_overloads_remain_separate_and_one_pass_never_certifies(self):
        report = coverage(inventory(), [case()], {"abs-normal": "PASS"})
        self.assertFalse(report["complete"])
        self.assertEqual(report["counts"], {"untested": 1, "implemented-unverified": 1, "failing": 0, "verified": 0})
        self.assertEqual(report["routines"][0]["missing_case_kinds"], ["boundary", "error", "null"])

    def test_duplicate_inventory_rejected(self):
        data = inventory()
        data["routines"].append(data["routines"][0])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_inventory(data)

    def test_missing_catalog_and_wrong_version_rejected(self):
        for mutation in ({"types": []}, {"server_version_num": 180001}, {"routines": []}):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_inventory(inventory() | mutation)

    def test_unknown_routine_and_duplicate_case_rejected(self):
        cases = [case()]
        cases[0]["routines"] = ["pg_catalog.missing()/f"]
        with self.assertRaisesRegex(ValueError, "unknown routine"):
            validate_cases(cases, validate_inventory(inventory()))
        with self.assertRaisesRegex(ValueError, "duplicate case"):
            validate_cases([case(), case()], validate_inventory(inventory()))

    def test_missing_extra_and_invalid_results_rejected(self):
        for results in ({}, {"abs-normal": "PASS", "extra": "PASS"}, {"abs-normal": "SKIPPED"}):
            with self.subTest(results=results), self.assertRaises(ValueError):
                coverage(inventory(), [case()], results)

    def test_untested_and_failed_cases_cannot_pass(self):
        for status in ("FAIL", "UNTESTED", "SETUP_ERROR", "ORACLE_ERROR"):
            report = coverage(inventory(), [case()], {"abs-normal": status})
            self.assertEqual(report["counts"]["failing"], 1)
            self.assertEqual(report["counts"]["verified"], 0)

    def test_matching_errors_do_not_pass(self):
        error = {"status": "error", "core_error": "Unsupported"}
        self.assertEqual(compare(case()["expected"], error, error), "ORACLE_ERROR")

    def test_types_values_nulls_and_row_counts_are_compared(self):
        expected = case()["expected"]
        for field, value in (("columns", [{"name": "v", "type": "integer"}]),
                             ("rows", [[None]]), ("rows", [[True]]), ("rows", [[1.0]]),
                             ("rows", [[2]]), ("rows", []), ("rows", [[1], [1]])):
            candidate = copy.deepcopy(expected)
            candidate[field] = value
            self.assertEqual(compare(expected, expected, candidate), "FAIL")
        self.assertEqual(compare(expected, expected, expected), "PASS")

    def test_setup_failures_are_distinct(self):
        expected = case()["expected"]
        self.assertEqual(compare(expected, expected, {"status": "setup_error"}), "SETUP_ERROR")

    def test_hash_is_order_independent_but_sensitive_to_signature(self):
        self.assertEqual(digest({"a": 1, "b": 2}), digest({"b": 2, "a": 1}))
        changed = inventory()
        changed["routines"][0]["identity"] = "pg_catalog.abs(numeric)/f"
        self.assertNotEqual(digest(inventory()), digest(changed))

    def test_reference_pin_detects_tampering(self):
        metadata = {"image": IMAGE, "server_version_num": 180000,
                    "routine_count": 2, "inventory_sha256": digest(inventory())}
        validate_pin(inventory(), metadata)
        for mutation in ({"image": "postgres:latest"}, {"routine_count": 1},
                         {"inventory_sha256": "0" * 64}, {"server_version_num": 170000}):
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "mismatch"):
                validate_pin(inventory(), metadata | mutation)

    def test_checked_in_catalog_and_cases_are_consistent(self):
        snapshot = json.loads(gzip.decompress((ROOT / "postgres-18.0.json.gz").read_bytes()))
        validate_pin(snapshot, json.loads((ROOT / "reference.json").read_text(encoding="utf-8")))
        validate_cases(json.loads((ROOT / "cases.json").read_text(encoding="utf-8")),
                       validate_inventory(snapshot))


class NegativeTests(unittest.TestCase):
    def test_setup_error_cannot_satisfy_negative(self):
        expected = {"status": "error", "sqlstate": "22003"}
        for marker, status in [("", "setup_error"), ("__DD68_SETUP_OK__", "error")]:
            result = decode_oracle(SimpleNamespace(returncode=3, stdout=marker, stderr="ERROR:  22003: out of range"))
            self.assertEqual(result["status"], status)
            self.assertEqual(compare(expected, result, {"status": "error", "core_error": "Sql(Overflow)"}, "Sql(Overflow)"),
                             "ORACLE_ERROR" if not marker else "PASS")

    def test_wrong_core_variant_and_fake_wire_state_fail(self):
        expected = {"status": "error", "sqlstate": "22003"}
        for candidate in [{"status": "error", "core_error": "Sql(Type)"},
                          {"status": "error", "sqlstate": "22003"},
                          {"status": "setup_error", "core_error": "Sql(Overflow)"}]:
            self.assertNotEqual(compare(expected, expected, candidate, "Sql(Overflow)"), "PASS")
        self.assertEqual(compare(expected, {"status":"error","sqlstate":"42883"},
                                 {"status":"error","core_error":"Sql(Overflow)"}, "Sql(Overflow)"), "ORACLE_ERROR")

    def test_execution_infrastructure_error_not_sql_error(self):
        result = decode_oracle(SimpleNamespace(returncode=1, stdout="__DD68_SETUP_OK__", stderr="connection lost"))
        self.assertEqual(result["status"], "oracle_error")

    def test_exact_precision_and_decimal_codec(self):
        result = decode_oracle(SimpleNamespace(returncode=0, stdout='__DD68_SETUP_OK__\n{"rows":[[9007199254740993,123456789.123456789]]}', stderr=""))
        self.assertEqual(result["rows"], [[9007199254740993, {"numeric_decimal":"123456789.123456789"}]])

    def test_parameter_literals_escape_text_and_keep_integer_precision(self):
        self.assertEqual(parameter_literal({"type":"text","value":"O'Reilly"}), "CAST('O''Reilly' AS text)")
        self.assertEqual(oracle_query({"query":"SELECT abs($1)", "params":[{"type":"bigint","value":-9007199254740993}]}),
                         "SELECT abs(CAST(-9007199254740993 AS bigint))")
        for param in [{"type":"bigint","value":True},{"type":"text","value":12},{"type":"float","value":1.0}]:
            with self.assertRaises(ValueError):
                parameter_literal(param)

    def test_missing_binding_is_not_last_parameter(self):
        for query in ["SELECT $0", "SELECT $2"]:
            with self.assertRaisesRegex(ValueError, "missing parameter"):
                oracle_query({"query":query,"params":[{"type":"integer","value":1}]})

    def test_success_requires_explicit_result_shape(self):
        for field in ["columns", "rows"]:
            c = case()
            del c["expected"][field]
            with self.assertRaises(ValueError):
                validate_cases([c], validate_inventory(inventory()))

    def test_negative_requires_both_explicit_expectations(self):
        c = case()
        c["expected"] = {"status":"error","sqlstate":"22003"}
        with self.assertRaises(ValueError):
            validate_cases([c], validate_inventory(inventory()))
        c["expected_core_error"] = "Sql(Overflow)"
        validate_cases([c], validate_inventory(inventory()))


class BindingTests(unittest.TestCase):
    def bind(self, sql, values=()):
        return oracle_query({"query": sql, "params": [{"type": "integer", "value": v} for v in values]})

    def test_opaque_regions_are_preserved(self):
        for region in ["'$1'", "'it''s $1'", '"$1"', '"a""$1"',
                       '$$$1$$', '$tag$$1 /* */ $tag$', '$T_$2 $t_$T_$',
                       "E'it\\'s $1'", "E'one'\n'it\\'s $1'", "E'one' -- gap\n'it\\'s $1'",
                       "e'\\\\ $1'", "'\\'", "U&'d\\0061 $1'",
                       '-- $1\n', '-- $1\r', '/* $1 /* $2 */ $3 */']:
            with self.subTest(region=region):
                self.assertEqual(self.bind('SELECT ' + region + ' , $1', [7]),
                                 'SELECT ' + region + ' , CAST(7 AS integer)')
        sql = "SELECT '$1', $$ $1 $$ -- $2"
        self.assertEqual(self.bind(sql), sql)

    def test_identifiers_can_contain_dollars(self):
        for sql in ['SELECT foo$1', 'SELECT foo$tag$bar', "SELECT aE'$1'"]:
            self.assertEqual(self.bind(sql), sql)

    def test_multi_digit_repeated_and_out_of_order_bindings(self):
        indices = [10, 1, 1, *range(2, 10)]
        sql = 'SELECT ' + ','.join('$'+str(i) for i in indices)
        expected = 'SELECT ' + ','.join('CAST('+str(i)+' AS integer)' for i in indices)
        self.assertEqual(self.bind(sql, list(range(1, 11))), expected)

    def test_missing_and_unused_bindings_rejected(self):
        for sql, params, error in [('SELECT $1', [], 'missing'), ('SELECT $0', [1], 'missing'),
                                   ('SELECT $2', [1], 'missing'), ('SELECT $2', [1, 2], 'unused'),
                                   ("SELECT '$1'", [1], 'unused'), ('SELECT 1', [1], 'unused')]:
            with self.subTest(sql=sql), self.assertRaisesRegex(ValueError, error):
                self.bind(sql, params)

    def test_malformed_lexical_inputs_rejected(self):
        for sql in ["SELECT 'abc", 'SELECT "abc', "SELECT E'abc\\'", 'SELECT $$abc',
                    'SELECT $tag$abc$TAG$', 'SELECT /* a /* b */', 'SELECT */',
                    'SELECT $', 'SELECT $bad', 'SELECT $1abc', 'SELECT $1$2', 'SELECT \x00']:
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                self.bind(sql, [1])

    def test_integer_width_boundaries_and_typed_nulls(self):
        for kind, bits in [('smallint', 16), ('integer', 32), ('bigint', 64)]:
            for value in [-(2**(bits-1)), 2**(bits-1)-1]:
                self.assertEqual(parameter_literal({'type': kind, 'value': value}), f'CAST({value} AS {kind})')
            for value in [-(2**(bits-1))-1, 2**(bits-1), True, 1.0, '1']:
                with self.subTest(kind=kind, value=value), self.assertRaises(ValueError):
                    parameter_literal({'type': kind, 'value': value})
            self.assertEqual(parameter_literal({'type': kind, 'value': None}), f'CAST(NULL AS {kind})')

    def test_actual_nul_rejected_but_backslash_text_preserved(self):
        with self.assertRaises(ValueError):
            parameter_literal({'type': 'text', 'value': 'a\x00b'})
        self.assertEqual(parameter_literal({'type': 'text', 'value': r'\x00'}), r"CAST('\x00' AS text)")

    def test_malformed_parameter_shapes_rejected(self):
        for param in [None, [], {}, {'type': 'text'}, {'type': [], 'value': 1},
                      {'type': 'integer', 'value': 1, 'extra': 1}]:
            with self.subTest(param=param), self.assertRaises(ValueError):
                parameter_literal(param)

    def test_injection_text_is_quoted_once_and_never_rescanned(self):
        value = "'); DROP TABLE x; -- $2 /* $$ \\"
        literal = "CAST('''); DROP TABLE x; -- $2 /* $$ \\' AS text)"
        self.assertEqual(parameter_literal({'type': 'text', 'value': value}), literal)
        self.assertEqual(oracle_query({'query': 'SELECT $1, $1', 'params': [{'type': 'text', 'value': value}]}),
                         'SELECT ' + literal + ', ' + literal)


@unittest.skipUnless(os.environ.get('DD68_REFERENCE_TESTS') == '1', 'set DD68_REFERENCE_TESTS=1 for pinned Docker reference')
class ReferenceBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reference = Reference()
        cls.reference.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.reference.__exit__(None, None, None)

    def test_live_lexical_binding_values_and_types(self):
        fixtures = [
            ("SELECT '$1' AS v, $1 AS n", ['$1', 7], ['text', 'integer']),
            ("SELECT $$ $1 $$ AS v, $1 AS n", [' $1 ', 7], ['text', 'integer']),
            ("SELECT $tag$$1$tag$ AS v, $1 AS n", ['$1', 7], ['text', 'integer']),
            ("SELECT E'it\\'s $1' AS v, $1 AS n", ["it's $1", 7], ['text', 'integer']),
            ("SELECT E'one'\n'it\\'s $1' AS v, $1 AS n", ["oneit's $1", 7], ['text', 'integer']),
            ("SELECT E'one' -- gap\n'it\\'s $1' AS v, $1 AS n", ["oneit's $1", 7], ['text', 'integer']),
            ("SELECT 'a''$1\\' AS v, $1 AS n", ["a'$1\\", 7], ['text', 'integer']),
            ('SELECT $1 AS "$1" /* $2 /* $3 */ $4 */ -- $5\n', [7], ['integer']),
        ]
        for sql, row, types in fixtures:
            with self.subTest(sql=sql):
                result = self.reference.query({'setup': [], 'query': sql, 'params': [{'type': 'integer', 'value': 7}]})
                self.assertEqual(result, {'status': 'ok', 'columns': [
                    {'name': name, 'type': kind} for name, kind in zip(
                        ['$1'] if len(types) == 1 else ['v', 'n'], types)], 'rows': [row]})

    def test_live_injection_text_and_repeated_multi_digit_parameters(self):
        value = "'); DROP TABLE x; -- $2 /* $$ \\"
        result = self.reference.query({'setup': [], 'query': 'SELECT $1 AS v',
                                       'params': [{'type': 'text', 'value': value}]})
        self.assertEqual(result, {'status': 'ok', 'columns': [{'name': 'v', 'type': 'text'}], 'rows': [[value]]})
        result = self.reference.query({'setup': [], 'query': 'SELECT ' + ','.join(
            '$'+str(i)+' AS v'+str(n) for n, i in enumerate([10, 1, 1, *range(2, 10)])),
            'params': [{'type': 'integer', 'value': i} for i in range(1, 11)]})
        self.assertEqual(result, {'status': 'ok', 'columns': [
            {'name': 'v'+str(n), 'type': 'integer'} for n in range(11)],
            'rows': [[10, 1, 1, *range(2, 10)]]})


if __name__ == "__main__":
    unittest.main()
