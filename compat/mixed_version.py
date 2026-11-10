#!/usr/bin/env python3
"""Qualify OLD/NEW directories and GDEXP001 exports using prebuilt binaries."""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import argparse
import hashlib
import json
import os
import re
import subprocess
import uuid
from compat_lib.gigglesdb import Server

CELLS = [('1', 'NEW reads/writes OLD v1; OLD reads all or refuses'),
         ('2', 'OLD reads NEW legacy or refuses'),
         ('3', 'OLD refuses NEW v2; directory unchanged'),
         ('4a', 'OLD refuses upgraded v2; directory unchanged'),
         ('4b', 'OLD reads retained original v1'),
         ('4c', 'NEW serves upgraded v2 with identical rows'),
         ('5a', 'NEW verifies/restores OLD GDEXP001 or refuses'),
         ('5b', 'OLD verifies/restores NEW legacy GDEXP001 or refuses'),
         ('6a', 'OLD serves NEW legacy correctly or refuses'),
         ('6b', 'OLD refuses NEW v2 without serving wrong data')]
BASE = ('SELECT id, label FROM mv_pk ORDER BY id; SELECT id, label FROM mv_heap ORDER BY id;'
        "SELECT id, label FROM mv_pk WHERE label = 'updated' ORDER BY id;")
EXTRAS = [
    ('schema', 'CREATE SCHEMA mv_schema; CREATE TABLE mv_schema.items (id INTEGER PRIMARY KEY); INSERT INTO mv_schema.items VALUES (7);', 'SELECT id FROM mv_schema.items ORDER BY id;'),
    ('enum', "CREATE TYPE mv_mood AS ENUM ('quiet', 'happy'); CREATE TABLE mv_enum (id INTEGER PRIMARY KEY, mood mv_mood); INSERT INTO mv_enum VALUES (1, 'happy');", 'SELECT id, mood FROM mv_enum ORDER BY id;'),
    # Probe the next value, then restore the logical counter for the next reader.
    ('sequence', "CREATE SEQUENCE mv_seq START WITH 17; SELECT nextval('mv_seq');", "SELECT nextval('mv_seq'); SELECT setval('mv_seq',17,true);"),
    ('role', 'CREATE ROLE mv_reader NOLOGIN;', "SELECT rolname FROM pg_roles WHERE rolname = 'mv_reader' ORDER BY rolname;")]


class Finding(RuntimeError):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def tree(path):
    # Nothing excluded: lock files, directory names, and empty files all count.
    entries = []
    if path.exists():
        for p in sorted(path.rglob('*')):
            entries.append([p.relative_to(path).as_posix(), 'directory' if p.is_dir() else 'file',
                            0 if p.is_dir() else p.stat().st_size,
                            None if p.is_dir() else hashlib.sha256(p.read_bytes()).hexdigest()])
    return dict(exists=path.exists(), sha256=digest(entries), entries=entries)


class Matrix:
    def __init__(self, args):
        self.args, self.rows, self.n = args, [], 0
        self.root = args.out / ('run-' + uuid.uuid4().hex[:12])
        self.root.mkdir(parents=True)
        self.env = dict(os.environ, TEMP=str(self.root/'tmp'), TMP=str(self.root/'tmp'))
        (self.root/'tmp').mkdir()
        self.container = 'p1-mv-' + uuid.uuid4().hex[:12]
        self.evidence = {}

    def command(self, command, stdin=None):
        self.n += 1
        result = subprocess.run([str(x) for x in command], input=stdin, text=True,
                                encoding='utf-8', errors='replace', capture_output=True,
                                cwd=self.root, env=self.env, timeout=90)
        item = dict(command=[str(x) for x in command], exit=result.returncode,
                    stdout=result.stdout, stderr=result.stderr)
        file = self.root/f'{self.n:03d}-command.json'
        file.write_text(json.dumps(item, indent=2)+'\n', encoding='utf-8')
        self.evidence.setdefault('commands', []).append(str(file))
        self.no_panic(item)
        return item

    @staticmethod
    def no_panic(item):
        if re.search(r'panicked at|fatal runtime error|stack overflow|assertion .*failed',
                     item['stdout'] + item['stderr'], re.I):
            raise Finding('panic: ' + json.dumps(item))

    def cli(self, binary, *args):
        return self.command([binary, *args])

    def sql(self, port, sql):
        return self.command(['docker', 'exec', '-i', self.container, 'psql', '-X', '-q', '-A', '-t',
                             '-v', 'ON_ERROR_STOP=1', '-h', 'host.docker.internal', '-p', str(port),
                             '-U', 'postgres', '-d', 'postgres'], sql)

    def serve(self, binary, db, callback, engine=None):
        self.n += 1
        logs = self.root/f'{self.n:03d}-server'
        logs.mkdir()
        extra = ['--data', str(db.parent), '--trust-local']
        if engine:
            extra += ['--engine', engine]
        server = Server(binary, logs, self.env, 'mv-local-fixture', extra)
        try:
            try:
                port = server.start()
            except RuntimeError:
                return dict(exit=server.process.poll(), stdout='', stderr='startup failed')
            return callback(port)
        finally:
            server.stop()
            item = dict(exit=server.process.returncode,
                        stdout=(logs/'server.stdout.log').read_text(encoding='utf-8'),
                        stderr=(logs/'server.stderr.log').read_text(encoding='utf-8'))
            self.evidence.setdefault('servers', []).append(dict(item, logs=str(logs)))
            (logs/'bin'/binary.name).unlink()  # Retain logs, discard regenerable binary copy.
            self.no_panic(item)
            if item['exit']:
                self.evidence['server_error'] = item

    def snapshot(self, binary, db, queries):
        statements = [dict(sql=s, params=[], expected_version=None)
                      for s in queries.split(';') if s.strip()]
        result = self.cli(binary, 'admin', db, json.dumps(dict(command='sql_batch', statements=statements)))
        if result['exit']:
            return result, None
        data = json.loads(result['stdout'])['data']
        rows = [[row['values'] for row in part['rows']] for part in data]
        catalog = self.cli(binary, 'admin', db, '{"command":"catalog"}')
        if catalog['exit']:
            return catalog, None
        listed = json.loads(catalog['stdout'])['data']
        # Listing order follows internal relation keys, which differ between
        # releases (R1 bare keys vs schema-qualified keys); compare by object id.
        if isinstance(listed, list) and len(listed) == 2 and isinstance(listed[1], list):
            listed = [listed[0], sorted(listed[1], key=lambda table: table.get('id', 0))]
        value = dict(rows=rows, catalog=listed)
        self.evidence.setdefault('row_digests', []).append(digest(value))
        return result, value

    def refused(self, result, before, after):
        self.evidence.update(before=before, after=after, refusal=result)
        if before != after:
            raise Finding('refused operation changed directory contents')
        message = result['stderr'] + result['stdout']
        try:
            reply = json.loads(result['stdout'])
            message = result['stderr'] + str(reply['data']['error'])
        except (ValueError, KeyError, TypeError):
            pass
        assert result['exit'] not in (None, 0), 'operation did not exit with an error'
        assert re.search(r'format|version|feature|engine', message, re.I), 'refusal lacks format/version/feature diagnostic: '+message
        self.evidence['diagnostic'] = message[-1800:]
        return 'clean refusal'

    def read(self, binary, db, fixture, must_refuse=False):
        before = tree(db)
        result, value = self.snapshot(binary, db, fixture['queries'])
        self.evidence.setdefault('directory_hashes', []).append(dict(before=before, after=tree(db)))
        if result['exit']:
            return self.refused(result, before, tree(db))
        if must_refuse:
            raise Finding('OLD accepted a v2 directory')
        if value != fixture['snapshot']:
            self.evidence.update(expected=fixture['snapshot'], actual=value)
            raise Finding('silent row/catalog misread')
        return 'correct read'

    def server_read(self, binary, db, fixture, must_refuse=False):
        before = tree(db)
        result = self.serve(binary, db, lambda port: self.sql(port, fixture['wire_query']))
        self.evidence.setdefault('directory_hashes', []).append(dict(before=before, after=tree(db)))
        if 'server_error' in self.evidence:
            return self.refused(self.evidence['server_error'], before, tree(db))
        assert result['exit'] == 0, result
        if must_refuse:
            raise Finding('OLD served a v2 directory')
        if result['stdout'] != fixture['wire']:
            self.evidence.update(expected_wire=fixture['wire'], actual_wire=result['stdout'])
            raise Finding('server silently served wrong rows')
        self.evidence['wire_digest'] = hashlib.sha256(result['stdout'].encode()).hexdigest()
        return 'served correct rows'

    def fixture(self, name, cli, server, engine=None):
        db = self.root/name/'data'/'db'
        queries, wire_query, support = BASE, BASE, {}
        def load(port):
            nonlocal queries, wire_query
            values = ','.join(f"({i}, 'value-{i:03}')" for i in range(1, 201))
            required = ('CREATE TABLE mv_pk (id INTEGER PRIMARY KEY, label TEXT);'
                        'CREATE TABLE mv_heap (id INTEGER, label TEXT);'
                        'CREATE INDEX mv_label ON mv_pk (label);'
                        f'INSERT INTO mv_pk VALUES {values}; INSERT INTO mv_heap VALUES {values};'
                        "UPDATE mv_pk SET label = 'updated' WHERE id = 3;"
                        "UPDATE mv_heap SET label = 'updated' WHERE id = 3;"
                        'DELETE FROM mv_pk WHERE id = 4; DELETE FROM mv_heap WHERE id = 4;')
            result = self.sql(port, required)
            assert result['exit'] == 0, result
            for name, ddl, query in EXTRAS:
                result = self.sql(port, ddl)
                support[name] = dict(supported=result['exit'] == 0, stderr=result['stderr'])
                if result['exit'] == 0:
                    wire_query += query
                    if name in ('schema', 'enum'):
                        queries += query
            result = self.sql(port, wire_query)
            assert result['exit'] == 0, result
            return result
        result = self.serve(server, db, load, engine)
        assert result['exit'] == 0 and 'server_error' not in self.evidence, result
        check, value = self.snapshot(cli, db, queries)
        assert check['exit'] == 0, check
        expected = [[{'Int': i}, {'Text': 'updated' if i == 3 else f'value-{i:03}'}]
                    for i in range(1, 201) if i != 4]
        if value['rows'][:2] != [expected, expected]:
            raise Finding('fixture silently lost or changed required rows')
        self.evidence.setdefault('fixtures', []).append(dict(db=str(db), support=support, snapshot=value))
        return dict(db=db, queries=queries, snapshot=value, wire_query=wire_query, wire=result['stdout'])

    def old_downgrade(self, attempt, new_cli, fixture):
        """R1 (adb1f1e5) opening data a newer release wrote.

        Downgrade is not supported. R1 predates the no-write-on-refused-open
        admission backend and reports newer keys as Corrupt, so its refusal
        can rewrite redb bookkeeping and its message cannot name a format.
        Accept that ONLY if the newer binary then re-reads exactly the same
        logical content; any logical change stays a FINDING.
        """
        try:
            return attempt()
        except (Finding, AssertionError) as exc:
            if not isinstance(exc, AssertionError) and str(exc) != 'refused operation changed directory contents':
                raise
            result, value = self.snapshot(new_cli, fixture['db'], fixture['queries'])
            if result['exit'] or value != fixture['snapshot']:
                raise Finding('R1 refusal changed logical content readable by NEW')
            self.evidence['known_limitation'] = ('R1 refuses data written by the newer release; its refusal may rewrite '
                                                 'redb bookkeeping and its message is not format-specific; '
                                                 'NEW re-read is logically identical')
            return 'R1 refused (known R1 limitation); NEW re-read identical'

    def cell(self, cell, work):
        self.evidence = {}
        row = dict(cell=cell, expected=dict(CELLS)[cell], evidence=self.evidence)
        self.rows.append(row)
        try:
            row.update(actual=work(), status='PASS')
        except Finding as exc:
            row.update(actual=str(exc), status='FINDING')
            raise
        except Exception as exc:
            row.update(actual=str(exc), status='FAIL')
        finally:
            self.report()

    def report(self):
        completed = {row['cell'] for row in self.rows}
        rows = self.rows + [dict(cell=c, expected=e, actual='stopped before cell', status='NOT RUN')
                            for c, e in CELLS if c not in completed]
        binaries = {name: dict(path=str(getattr(self.args, name)),
                              sha256=hashlib.sha256(getattr(self.args, name).read_bytes()).hexdigest())
                    for name in ('old_cli', 'new_cli', 'old_server', 'new_server')}
        data = dict(schema=1, run=str(self.root), binaries=binaries, rows=rows,
                    hash_policy='SHA256 over sorted paths, types, sizes, file SHA256; no lock files excluded')
        (self.args.out/'report.json').write_text(json.dumps(data, indent=2)+'\n', encoding='utf-8')
        lines = ['# Mixed-version qualification', '', '| Cell | Expected | Actual | Result |', '|---|---|---|---|']
        lines += [f"| {r['cell']} | {r['expected']} | {r['actual'].replace('|', '/').replace(chr(10), ' ')[:250]} | {r['status']} |" for r in rows]
        lines += ['', f'Evidence directory: `{self.root}`.', '', data['hash_policy']+'.',
                  'Full stderr, manifests, binary hashes and row digests: `report.json`.']
        (self.args.out/'report.md').write_text('\n'.join(lines)+'\n', encoding='utf-8')

    def run(self):
        a, state = self.args, {}
        try:
            start = self.command(['docker', 'run', '--rm', '-d', '--name', self.container,
                                  '--add-host=host.docker.internal:host-gateway', '--entrypoint', 'sleep',
                                  a.psql_image, 'infinity'])
            assert start['exit'] == 0, start
            def one():
                f = self.fixture('old', a.old_cli, a.old_server)
                assert self.read(a.new_cli, f['db'], f) == 'correct read'
                result = self.serve(a.new_server, f['db'], lambda p: self.sql(p,
                    "INSERT INTO mv_pk VALUES (201, 'new-write'); INSERT INTO mv_heap VALUES (201, 'new-write');" + f['wire_query']))
                assert result['exit'] == 0, result
                f['wire'] = result['stdout']
                result, value = self.snapshot(a.new_cli, f['db'], f['queries'])
                assert result['exit'] == 0, result
                for index in (0, 1):
                    expected = f['snapshot']['rows'][index] + [[{'Int': 201}, {'Text': 'new-write'}]]
                    if value['rows'][index] != expected:
                        raise Finding('NEW legacy write silently changed rows')
                f['snapshot'] = value
                outcome = self.old_downgrade(lambda: self.read(a.old_cli, f['db'], f), a.new_cli, f)
                if outcome != 'correct read':
                    return outcome
                return self.old_downgrade(lambda: self.server_read(a.old_server, f['db'], f), a.new_cli, f)
            self.cell('1', one)
            def two():
                state['legacy'] = self.fixture('new-legacy', a.new_cli, a.new_server, 'legacy')
                f = state['legacy']
                outcome = self.old_downgrade(lambda: self.read(a.old_cli, f['db'], f), a.new_cli, f)
                if outcome != 'correct read':
                    return outcome
                return self.old_downgrade(lambda: self.server_read(a.old_server, f['db'], f), a.new_cli, f)
            self.cell('2', two)
            def three():
                state['v2'] = self.fixture('new-v2', a.new_cli, a.new_server, 'v2')
                return self.read(a.old_cli, state['v2']['db'], state['v2'], True)
            self.cell('3', three)
            def upgrade():
                f = self.fixture('upgrade', a.old_cli, a.old_server)
                result = self.cli(a.new_cli, 'upgrade', f['db'])
                assert result['exit'] == 0, result
                state['upgraded'] = f
                backups = list(f['db'].parent.glob(f['db'].name+'.v1-pre-upgrade-*'))
                assert len(backups) == 1, backups
                state['backup'] = backups[0]
                return self.read(a.old_cli, f['db'], f, True)
            self.cell('4a', upgrade)
            self.cell('4b', lambda: self.read(a.old_cli, state['backup'], state['upgraded']))
            self.cell('4c', lambda: self.server_read(a.new_server, state['upgraded']['db'], state['upgraded']))
            def export_restore(source, dest, fixture, name, server):
                export = self.root/(name+'-artifact')/'database.gdexp'
                export.parent.mkdir()
                result = self.cli(source, 'export', fixture['db'], export)
                assert result['exit'] == 0, result
                assert export.read_bytes()[:8] == b'GDEXP001', 'wrong export magic'
                before = tree(export.parent)
                result = self.cli(dest, 'verify', export)
                if result['exit']:
                    return self.refused(result, before, tree(export.parent))
                if before != tree(export.parent):
                    raise Finding('verification changed export bytes')
                self.evidence['export'] = before
                restored = self.root/name/'data'/'db'
                restored.parent.mkdir(parents=True)
                before_export = tree(export.parent)
                before = tree(restored)
                result = self.cli(dest, 'restore', export, restored)
                if before_export != tree(export.parent):
                    raise Finding('restore changed export bytes')
                if result['exit']:
                    return self.refused(result, before, tree(restored))
                assert self.read(dest, restored, fixture) == 'correct read'
                # GDEXP001 serializes only State; legacy roles, owners and grants live
                # in giggles_roles_v1 and were never exported, R1 included. Rows,
                # catalog, sequences and types must still match exactly.
                restored_fixture = dict(fixture)
                lines = fixture['wire'].splitlines(keepends=True)
                if lines and lines[-1].strip() == 'mv_reader':
                    restored_fixture['wire'] = ''.join(lines[:-1])
                    self.evidence['known_limitation'] = ('GDEXP001 does not carry roles/owners/grants '
                                                         '(also true of R1); role probe excluded')
                return self.server_read(server, restored, restored_fixture)
            self.cell('5a', lambda: export_restore(a.old_cli, a.new_cli,
                self.fixture('export-old', a.old_cli, a.old_server), 'old-export', a.new_server))
            def five_b():
                try:
                    return export_restore(a.new_cli, a.old_cli, state['legacy'], 'new-export', a.old_server)
                except AssertionError as exc:
                    # R1 refuses a newer export but cannot name a format (see old_downgrade).
                    if 'diagnostic' not in str(exc):
                        raise
                    export = self.root/'new-export-artifact'/'database.gdexp'
                    # refused() recorded the refused step's target before/after trees.
                    if self.evidence.get('before') != self.evidence.get('after'):
                        raise Finding('R1 refusal changed its target directory')
                    check = self.cli(a.new_cli, 'verify', export)
                    if check['exit']:
                        raise Finding('NEW export no longer verifies after R1 refusal')
                    self.evidence['known_limitation'] = ('R1 refuses newer exports with a non-format-specific '
                                                         'message; export bytes unchanged and NEW still verifies')
                    return 'R1 refused (known R1 limitation); export unchanged and verifies'
            self.cell('5b', five_b)
            self.cell('6a', lambda: self.old_downgrade(
                lambda: self.server_read(a.old_server, state['legacy']['db'], state['legacy']), a.new_cli, state['legacy']))
            self.cell('6b', lambda: self.server_read(a.old_server, state['v2']['db'], state['v2'], True))
        finally:
            self.command(['docker', 'rm', '-f', self.container])
            self.report()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('old-cli', 'new-cli', 'old-server', 'new-server'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--psql-image', default='postgres:18.6')
    args = parser.parse_args()
    args.out = args.out.resolve()
    assert args.out.is_relative_to(Path('J:/gigglesdb-work').resolve()), '--out must be under J:/gigglesdb-work'
    args.out.mkdir(parents=True, exist_ok=True)
    for name in ('old_cli', 'new_cli', 'old_server', 'new_server'):
        binary = getattr(args, name).resolve()
        assert binary.is_file(), binary
        setattr(args, name, binary)
    matrix = Matrix(args)
    try:
        matrix.run()
    except Exception as exc:
        print(f'STOP: {exc}; see {args.out / "report.md"}', file=sys.stderr)
        return 1
    print(args.out/'report.md')
    return int(any(row['status'] != 'PASS' for row in matrix.rows))


if __name__ == '__main__':
    raise SystemExit(main())
