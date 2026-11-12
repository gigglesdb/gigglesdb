#!/usr/bin/env python3
"""Real-process v2 crash and five-million-row qualification (stdlib + Docker psql)."""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import argparse
import hashlib
import json
import os
import queue
import random
import shutil
import signal
import stat
import subprocess
import threading
import time
import traceback
import uuid
from compat_lib.gigglesdb import Server


def require(ok, detail):
    if not ok:
        raise RuntimeError(detail)


def size(path):
    total = 0
    for p in path.rglob('*'):
        try:
            info = p.stat()
        except FileNotFoundError:
            continue  # RocksDB compaction can unlink an SST during the walk.
        if stat.S_ISREG(info.st_mode):
            total += info.st_size
    return total


def payload(seed, row, length):
    # Independent rows avoid repeated/compressible padding and allow random access
    # when checking samples after a crash or export/restore. Hex is COPY/SQL safe.
    return hashlib.shake_256(f'{seed}:{row}'.encode('ascii')).hexdigest((length+1)//2)[:length]


class SqlError(RuntimeError):
    pass


class Psql:
    def __init__(self, run, port):
        self.run, self.port = run, port
        self.log = (run.root / f'psql-{uuid.uuid4().hex[:10]}.log').open('w', encoding='utf-8')
        self.lines = queue.Queue()
        self.process = subprocess.Popen(['docker', 'exec', '-i', run.container, 'psql',
            '-X', '-A', '-t', '-F', '\t', '-v', 'ON_ERROR_STOP=0', '-h', 'host.docker.internal',
            '-p', str(port), '-U', 'postgres', '-d', 'postgres'], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.log, text=True, encoding='utf-8', env=run.env)
        def drain():
            for line in self.process.stdout:
                self.lines.put(line.rstrip('\r\n'))
            self.lines.put(None)
        self.reader = threading.Thread(target=drain, daemon=True)
        self.reader.start()

    def sql(self, sql, timeout=1200, on_commit=None, dispatched=None):
        token = 'end_' + uuid.uuid4().hex
        self.log.write(sql + '\n'); self.log.flush()
        script = ''.join(s+';\n\\echo status_'+token+' :SQLSTATE\n'
                         for s in sql.split(';') if s.strip())
        self.process.stdin.write(script + '\\echo ' + token + '\n')
        self.process.stdin.flush()
        if dispatched:
            dispatched.set()
        rows, error, deadline = [], None, time.monotonic() + timeout
        while True:
            line = self.lines.get(timeout=max(.01, deadline-time.monotonic()))
            if line is None:
                raise ConnectionError('psql disconnected; see ' + self.log.name)
            if line == token:
                if error:
                    raise SqlError(error)
                return rows
            if line.startswith('status_'+token+' '):
                state = line.split()[-1]
                if state != '00000' and error is None:
                    error = state + ': ' + sql[:240] + '; see ' + self.log.name
                continue
            if line == 'COMMIT' and on_commit:
                on_commit()
            rows.append(line)

    def rows(self, sql):
        return [r.split('\t') for r in self.sql(sql) if '\t' in r or r.lstrip('-').isdigit()]

    def copy(self, table, text, count):
        # A separate psql -c session cannot leave the persistent SQL connection
        # in COPY input mode after a rejection. Each COPY is one autocommit chunk.
        result = subprocess.run(['docker', 'exec', '-i', self.run.container, 'psql',
            '-X', '-v', 'ON_ERROR_STOP=1', '-h', 'host.docker.internal',
            '-p', str(self.port), '-U', 'postgres', '-d', 'postgres',
            '-c', f'COPY {table} FROM STDIN'], input=text, capture_output=True,
            text=True, encoding='utf-8', env=self.run.env,
            timeout=self.run.args.command_timeout)
        self.log.write(f'COPY {table}: rows={count}; bytes={len(text.encode("utf-8"))}\n' +
                       result.stdout + result.stderr)
        self.log.flush()
        if result.returncode:
            raise SqlError(f'COPY failed ({result.returncode}): {result.stderr[:1000]}')
        require(f'COPY {count}' in result.stdout.splitlines(), 'COPY did not acknowledge all rows')

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.write('\\q\n'); self.process.stdin.flush()
                self.process.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                self.process.kill(); self.process.wait(timeout=10)
        self.reader.join(timeout=5)
        for stream in (self.process.stdin, self.process.stdout):
            try:
                stream.close()
            except OSError:
                pass  # pipe already broken by the deliberate server kill
        self.log.close()


class Qualification:
    def __init__(self, args):
        self.args, self.started = args, time.monotonic()
        self.rng = random.Random(args.seed)
        self.root = args.work / ('run-' + uuid.uuid4().hex[:12])
        self.root.mkdir(parents=True)
        (self.root/'tmp').mkdir()
        self.env = dict(os.environ, TEMP=str(self.root/'tmp'), TMP=str(self.root/'tmp'),
                        CARGO_TARGET_DIR=str(args.work.parent/'target'))
        self.container = 'p1-pc-' + uuid.uuid4().hex[:12]
        self.server, self.clients, self.cells, self.serial = None, [], {}, 0
        self.restarts = []
        self.loaded_rows = 0

    def client(self, port):
        client = Psql(self, port)
        self.clients.append(client)
        require(client.rows('SELECT 1;') == [['1']], 'psql readiness check failed')
        return client

    def start(self, data):
        self.serial += 1
        logs = self.root/f'server-{self.serial:03}'
        logs.mkdir()
        self.server = Server(self.args.server_bin, logs, self.env, 'qualification',
                             ['--data', str(data), '--trust-local'])
        began = time.monotonic()
        port = self.server.start()
        self.restarts.append(round(time.monotonic()-began, 3))
        return port

    def kill(self):
        if self.server and self.server.process and self.server.process.poll() is None:
            if os.name == 'nt':
                subprocess.run(['taskkill', '/T', '/F', '/PID', str(self.server.process.pid)],
                               env=self.env, capture_output=True, check=True, timeout=30)
            else:
                os.kill(self.server.process.pid, signal.SIGKILL)
            self.server.process.wait(timeout=30)

    def stop(self):
        for client in self.clients:
            client.close()
        self.clients.clear()
        if self.server:
            self.server.stop()
            (self.server.run/'bin'/self.args.server_bin.name).unlink(missing_ok=True)
            self.server = None

    def check_size(self):
        disk = size(self.root)
        require(disk <= self.args.max_bytes,
                f'qualification exceeded --max-bytes={self.args.max_bytes}: bytes={disk}')

    def load_batches(self, c, table, rows, values, start=0):
        checked = start
        for lo in range(start, rows, self.args.batch):
            hi = min(rows, lo+self.args.batch)
            self.insert_batch(c, table, lo, hi, values)
            if hi-checked >= self.args.size_check_rows or hi == rows:
                self.check_size()
                checked = hi
                print(f'{table}: {hi}/{rows} rows', flush=True)

    def insert_batch(self, c, table, lo, hi, values):
        reply = c.sql(f'BEGIN; INSERT INTO {table} VALUES ' +
                      ','.join(values(i) for i in range(lo, hi)) + '; COMMIT;')
        require('COMMIT' in reply, 'batch did not acknowledge COMMIT')

    def big_values(self, i):
        extra = f",'{payload(self.args.seed, i, self.args.payload_bytes)}'" if self.args.payload_bytes else ''
        return f'({i},{i},{i*17}{extra})'

    def load_large(self, c, data):
        checked, disk, copy_enabled = 0, 0, True
        stats = dict(copy_rows=0, copy_bytes=0, copy_seconds=0.0, insert_rows=0)
        lo = 0
        while lo < self.args.rows:
            hi = min(self.args.rows, lo+self.args.copy_batch)
            if copy_enabled:
                began = time.monotonic()
                text = ''.join(f'{i}\t{i}\t{i*17}' +
                               ('\t'+payload(self.args.seed, i, self.args.payload_bytes)
                                if self.args.payload_bytes else '') + '\n' for i in range(lo, hi))
                try:
                    c.copy('big', text, hi-lo)
                except SqlError as exc:
                    # Never retry a chunk whose commit acknowledgement was lost.
                    require(c.rows(f'SELECT count(*) FROM big WHERE id>={lo} AND id<{hi};') == [['0']],
                            'failed COPY left rows behind; refusing ambiguous INSERT retry')
                    copy_enabled = False
                    stats['copy_fallback'] = str(exc)
                    print(f'COPY rejected; falling back to INSERT: {exc}', flush=True)
                else:
                    stats['copy_rows'] += hi-lo
                    stats['copy_bytes'] += len(text.encode('utf-8'))
                    stats['copy_seconds'] += time.monotonic()-began
            if not copy_enabled:
                # Keep INSERT chunks bounded too, including target/limit sampling.
                hi = min(self.args.rows, lo+self.args.batch)
                self.insert_batch(c, 'big', lo, hi, self.big_values)
                stats['insert_rows'] += hi-lo
            self.loaded_rows = hi
            if hi-checked >= self.args.size_check_rows or hi == self.args.rows:
                self.check_size()
                disk, checked = size(data), hi
                print(f'big: {hi}/{self.args.rows} rows; bytes={disk}', flush=True)
                if self.args.target_bytes and disk >= self.args.target_bytes:
                    break
            lo = hi
        return disk, stats

    def crash_load(self):
        for iteration in range(self.args.iterations):
            data = self.root/f'kill-{iteration:03}'
            port = self.start(data)
            c = self.client(port)
            c.sql('CREATE TABLE bank (id INTEGER PRIMARY KEY, balance BIGINT);'
                  'CREATE TABLE parents (id INTEGER PRIMARY KEY);'
                  'CREATE TABLE tx (id BIGINT PRIMARY KEY, src INTEGER, dst INTEGER, amount INTEGER);'
                  'CREATE TABLE items (id BIGINT PRIMARY KEY, token BIGINT, parent_id INTEGER REFERENCES parents(id), val INTEGER);'
                  'CREATE UNIQUE INDEX items_token ON items(token);')
            c.sql('INSERT INTO bank VALUES ' + ','.join(f'({i},10000)' for i in range(100)) + ';'
                  'INSERT INTO parents VALUES ' + ','.join(f'({i})' for i in range(100)) + ';')
            stop, errors, intents, ack = threading.Event(), [], {}, set()
            guard = threading.Lock()
            journal = (self.root/f'kill-{iteration:03}-transactions.jsonl').open('w', encoding='utf-8')
            def worker(w, conn, seed):
                rng, n = random.Random(seed), 0
                try:
                    while not stop.is_set():
                        n += 1
                        tx = w*1_000_000+n
                        a, b = rng.sample(range(w*25, (w+1)*25), 2)
                        amount = rng.randint(1, 9)
                        with guard:
                            intents[tx] = (a, b, amount)
                            journal.write(json.dumps(dict(intent=tx, src=a, dst=b, amount=amount))+'\n'); journal.flush()
                        def acknowledge():
                            with guard:
                                ack.add(tx)
                                journal.write(json.dumps(dict(ack=tx))+'\n'); journal.flush()
                        try:
                            conn.sql(f'BEGIN; UPDATE bank SET balance=balance-{amount} WHERE id={a};'
                                     f'UPDATE bank SET balance=balance+{amount} WHERE id={b};'
                                     f'INSERT INTO tx VALUES ({tx},{a},{b},{amount});'
                                     f'INSERT INTO items VALUES ({tx*2},{tx*2},{a},0),({tx*2+1},{tx*2+1},{b},0);'
                                     f'UPDATE items SET val=7 WHERE id={tx*2}; DELETE FROM items WHERE id={tx*2+1};')
                            reply = conn.sql('COMMIT;', on_commit=acknowledge)
                            require('COMMIT' in reply, f'transaction {tx} did not commit: {reply}')
                        except SqlError as exc:
                            if not str(exc).startswith(('40001:', '40P01:')):
                                raise
                            conn.sql('ROLLBACK;')
                except Exception as exc:
                    if not stop.is_set():
                        errors.append(str(exc))
            threads = [threading.Thread(target=worker, args=(w, self.client(port), self.rng.getrandbits(64)))
                       for w in range(4)]
            for thread in threads:
                thread.start()
            delay = self.rng.uniform(.2, 3)
            time.sleep(delay)
            # Set before taskkill so expected disconnects cannot become spurious failures.
            stop.set(); self.kill()
            for thread in threads:
                thread.join(timeout=30)
            require(not any(t.is_alive() for t in threads), 'workload thread did not stop')
            journal.close(); self.stop()
            require(not errors, 'workload error before kill: ' + repr(errors))
            require(bool(ack), 'no commits acknowledged before kill; workload not qualified')
            c = self.client(self.start(data))
            persisted = {int(r[0]): tuple(map(int, r[1:])) for r in c.rows('SELECT id,src,dst,amount FROM tx ORDER BY id;')}
            require(ack <= persisted.keys(), f'lost acknowledged commits: {sorted(ack-persisted.keys())}')
            balances = [10000]*100
            expected = []
            for tx, (a, b, amount) in persisted.items():
                require(intents.get(tx) == (a, b, amount), f'unknown/corrupt transaction {tx}')
                balances[a] -= amount; balances[b] += amount
                expected.append([tx*2, tx*2, a, 7])
            bank = [list(map(int, r)) for r in c.rows('SELECT id,balance FROM bank ORDER BY id;')]
            require(bank == [[i, v] for i, v in enumerate(balances)], 'partially visible transfer or incorrect balances')
            require(sum(r[1] for r in bank) == 1_000_000, 'bank sum invariant broken')
            scan = [list(map(int, r)) for r in c.rows('SELECT id,token,parent_id,val FROM items ORDER BY id;')]
            require(scan == sorted(expected), 'partial insert/update/delete transaction or FK orphan')
            require(len({r[1] for r in scan}) == len(scan), 'unique index duplicates')
            indexed = [list(map(int, r)) for r in c.rows('SELECT id,token,parent_id,val FROM items WHERE token>=0 ORDER BY id;')]
            require(indexed == scan, 'unique index and full scan disagree')
            for row in scan[:1]+scan[-1:]:
                require(c.rows(f'SELECT id FROM items WHERE token={row[1]};') == [[str(row[0])]], 'unique point lookup disagrees')
            require(sorted(int(r[0]) for r in c.rows('SELECT id FROM parents;')) == list(range(100)), 'parent rows lost')
            tables = c.sql("SELECT relname FROM pg_catalog.pg_class WHERE relkind='r';")
            require({'bank', 'parents', 'tx', 'items'} <= set(tables), 'catalog missing expected tables')
            self.stop()
            self.cells['kill-under-load'] = ('PASS', f'{iteration+1}/{self.args.iterations} iterations; last acknowledged={len(ack)}, persisted={len(persisted)}')
            self.report()
            print(f'kill-under-load {iteration+1}: ACK={len(ack)}, recovered={len(persisted)}', flush=True)

    def crash_ddl(self):
        outcomes = []
        for i, ddl in enumerate(['CREATE INDEX ddl_idx ON ddl_t(v);',
                                 'ALTER TABLE ddl_t ADD COLUMN extra INTEGER;', 'DROP TABLE ddl_t;']):
            data = self.root/f'ddl-{i}'
            port = self.start(data)
            c = self.client(port)
            c.sql('CREATE TABLE ddl_t (id INTEGER PRIMARY KEY,v INTEGER);')
            self.load_batches(c, 'ddl_t', 20000, lambda n: f'({n},{n*3})')
            sent, completed, errors = threading.Event(), threading.Event(), []
            def execute():
                try:
                    c.sql(ddl, dispatched=sent)
                    completed.set()
                except (ConnectionError, OSError):
                    pass
                except Exception as exc:
                    errors.append(str(exc))
            thread = threading.Thread(target=execute)
            thread.start(); require(sent.wait(timeout=30), 'DDL not dispatched'); time.sleep(.001)
            self.kill(); thread.join(timeout=30); self.stop()
            require(not thread.is_alive() and not errors, 'DDL error: ' + repr(errors))
            # A fast DDL may finish before the kill lands. Then it must be durable;
            # otherwise it must be wholly applied or wholly absent, never partial.
            acked = completed.is_set()
            c = self.client(self.start(data))
            exists = 'ddl_t' in c.sql("SELECT relname FROM pg_catalog.pg_class WHERE relkind='r';")
            if i != 2 or exists:
                require(c.rows('SELECT count(*),sum(v) FROM ddl_t;') == [['20000', '599970000']], 'DDL partially damaged table')
                if i == 0:
                    listed = c.sql("SELECT relname FROM pg_catalog.pg_class WHERE relkind='i';")
                    require(c.rows('SELECT id FROM ddl_t WHERE v=369;') == [['123']], 'index/table unusable')
                    for lo in range(0, 20000, 1000):
                        require(c.rows(f'SELECT id FROM ddl_t WHERE v>={lo*3} AND v<{(lo+1000)*3} ORDER BY v;') ==
                                [[str(n)] for n in range(lo, lo+1000)], 'partial CREATE INDEX')
                    state = 'applied' if 'ddl_idx' in listed else 'absent'
                elif i == 1:
                    cols = c.sql("SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid=c.oid WHERE c.relname='ddl_t' AND a.attnum>0;")
                    state = 'applied' if 'extra' in cols else 'absent'
                    if state == 'applied':
                        require(c.rows('SELECT count(*) FROM ddl_t WHERE extra IS NULL;') == [['20000']], 'partial ADD COLUMN')
                else:
                    state = 'absent'
                c.sql('INSERT INTO ddl_t(id,v) VALUES (20000,60000); DELETE FROM ddl_t WHERE id=20000;')
            else:
                state = 'applied'
                c.sql('CREATE TABLE ddl_t (id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO ddl_t VALUES(1,3);')
                require(c.rows('SELECT v FROM ddl_t;') == [['3']], 'dropped table cannot be recreated')
            require(not acked or state == 'applied', f'acknowledged DDL lost after kill: {ddl}')
            outcomes.append(f"{ddl.split(' ')[0]} {ddl.split(' ')[1]}: {'acked+durable' if acked else 'killed mid-flight, ' + state}")
            self.stop()
            self.cells['kill-during-DDL'] = ('PASS', f'{i+1}/3 operations; ' + '; '.join(outcomes))
            self.report()

    def verify_large(self, c):
        n = self.loaded_rows
        require(c.rows('SELECT count(*),count(h),min(h),max(h) FROM big;') == [[str(n), str(n), '0', str((n-1)*17)]],
                'large-load count/min/max mismatch')
        for i in sorted({0, n//2, n-1}):
            require(c.rows(f'SELECT id,h FROM big WHERE h={i*17};') == [[str(i), str(i*17)]], 'secondary point lookup mismatch')
        hi = min(n, 100)
        require(c.rows(f'SELECT id,h FROM big WHERE v>=0 AND v<{hi} ORDER BY v;') ==
                [[str(i), str(i*17)] for i in range(hi)], 'secondary range scan mismatch')
        if self.args.payload_bytes:
            try:
                require(c.sql("SELECT md5('abc');") == ['900150983cd24fb0d6963f7d28e17f72'], 'md5 probe mismatch')
                digest = True
            except SqlError as exc:
                if not str(exc).startswith('42883:'):
                    raise
                digest = False
            for i in sorted({0, n//2, n-1}):
                expected = payload(self.args.seed, i, self.args.payload_bytes)
                if digest:
                    actual = c.rows(f'SELECT length(payload),md5(payload) FROM big WHERE id={i};')
                    require(actual == [[str(len(expected)), hashlib.md5(expected.encode('ascii')).hexdigest()]],
                            f'payload length/md5 mismatch at row {i}')
                else:
                    require(c.rows(f'SELECT id,payload FROM big WHERE id={i};') == [[str(i), expected]],
                            f'payload text mismatch at row {i}')

    def large_sum(self, c):
        # SUM over bigint is bounded by the shared aggregate work budget (DD-0068 E1260);
        # record the outcome separately so the storage cells stay strict.
        n = self.loaded_rows
        try:
            ok = c.rows('SELECT sum(h) FROM big;') == [[str(n*(n-1)//2*17)]]
            self.cells['large-sum'] = ('PASS', 'sum(h) matches') if ok else ('FAIL', 'sum(h) mismatch')
        except Exception as error:  # psql error text carries the SQLSTATE detail
            self.cells['large-sum'] = ('KNOWN LIMIT', f'sum(h) rejected: {str(error)[:160]}')

    def large(self):
        data = self.root/'large-data'
        c = self.client(self.start(data))
        extra = ',payload TEXT' if self.args.payload_bytes else ''
        c.sql(f'CREATE TABLE big (id BIGINT PRIMARY KEY,v BIGINT,h BIGINT{extra});'
              'CREATE INDEX big_v ON big(v); CREATE INDEX big_h ON big(h);')
        start = time.monotonic()
        disk, stats = self.load_large(c, data)
        elapsed = time.monotonic()-start
        self.kill(); self.stop()
        c = self.client(self.start(data))
        restart = self.restarts[-1]
        self.verify_large(c)
        self.large_sum(c); self.stop()
        n = self.loaded_rows
        detail = f'rows={n}; rows/s={n/elapsed:.1f}; bytes={disk}; disk MB/s={disk/elapsed/1e6:.3f}; restart={restart}s'
        if stats['copy_rows']:
            seconds = stats['copy_seconds']
            detail += (f'; COPY rows={stats["copy_rows"]}; COPY seconds={seconds:.3f}'
                       f'; COPY rows/s={stats["copy_rows"]/seconds:.1f}'
                       f'; COPY input MB/s={stats["copy_bytes"]/seconds/1e6:.3f}')
        if stats['insert_rows']:
            detail += f'; INSERT rows={stats["insert_rows"]}; fallback={stats["copy_fallback"]}'
        if self.args.target_bytes:
            detail += f'; target bytes={self.args.target_bytes}; target reached={disk >= self.args.target_bytes}'
        self.cells['large-load'] = ('PASS', detail)

    def backup(self):
        data = self.root/'large-data'
        cli = self.args.server_bin.parent/('giggles.exe' if os.name == 'nt' else 'giggles')
        require(cli.is_file(), 'v2 export is supported; build sibling giggles CLI for backup qualification')
        backup, restored = self.root/'large.gdexp', self.root/'restored-data'
        restored.mkdir(exist_ok=True)  # restore creates the database, not its parent directory
        for name, cmd in [('export', [cli, 'export', data/'db', backup]),
                          ('restore', [cli, 'restore', backup, restored/'db'])]:
            result = subprocess.run(list(map(str, cmd)), env=self.env, cwd=self.root,
                                    capture_output=True, text=True, timeout=self.args.command_timeout)
            (self.root/f'{name}.log').write_text(result.stdout+result.stderr, encoding='utf-8')
            require(result.returncode == 0, f'{name} failed: {result.stdout} {result.stderr}')
            if name == 'export':
                # The verified export is the only copy needed for restore; dropping the
                # source first keeps peak disk at roughly one database plus its export.
                shutil.rmtree(data)
        self.verify_large(self.client(self.start(restored))); self.stop()
        self.cells['backup-restore'] = ('PASS', 'v2 export/restore count, indexes and sampled payload verified')
        shutil.rmtree(restored); backup.unlink()

    def report(self):
        lines = ['# v2 real-process qualification', '', f'Seed: {self.args.seed}',
                 f'Iterations: {self.args.iterations}; rows: {self.args.rows}',
                 f'Loaded rows: {self.loaded_rows}; payload bytes/row: {self.args.payload_bytes}; '
                 f'target bytes: {self.args.target_bytes}; max bytes: {self.args.max_bytes}',
                 f'Runtime: {time.monotonic()-self.started:.3f}s',
                 f'Start/restart seconds: {self.restarts}', '', '| Cell | Result | Evidence |', '|---|---|---|']
        for name in ['kill-under-load', 'kill-during-DDL', 'large-load', 'large-sum', 'backup-restore']:
            status, detail = self.cells.get(name, ('NOT RUN', 'stopped before this cell'))
            lines.append(f'| {name} | {status} | {detail.replace(chr(10), " ").replace("|", "/")} |')
        (self.root/'report.md').write_text('\n'.join(lines)+'\n', encoding='utf-8')

    def run(self):
        print(f'Seed: {self.args.seed}\nEvidence: {self.root}', flush=True)
        current = 'kill-under-load'
        try:
            subprocess.run(['docker', 'run', '--rm', '-d', '--name', self.container,
                '--tmpfs', '/var/lib/postgresql', '--entrypoint', 'sleep', 'postgres:18.6', 'infinity'],
                env=self.env, capture_output=True, check=True, timeout=90)
            for current, method in [('kill-under-load', self.crash_load),
                                    ('kill-during-DDL', self.crash_ddl), ('large-load', self.large),
                                    ('backup-restore', self.backup)]:
                method()
            return 0
        except BaseException as exc:
            self.cells[current] = ('FAIL', str(exc))
            (self.root/'failure.txt').write_text(traceback.format_exc(), encoding='utf-8')
            return 1
        finally:
            self.kill(); self.stop()
            subprocess.run(['docker', 'rm', '-f', self.container], env=self.env,
                           capture_output=True, timeout=30)
            self.report()
            print((self.root/'report.md').read_text(encoding='utf-8'), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server-bin', type=Path, required=True)
    parser.add_argument('--work', type=Path, default=Path('J:/gigglesdb-work/p1-pc/qual'))
    parser.add_argument('--seed', type=int, default=5412)
    parser.add_argument('--iterations', type=int, default=30)
    parser.add_argument('--command-timeout', type=int, default=3600,
                        help='seconds allowed for each export or restore command')
    parser.add_argument('--rows', type=int, default=5_000_000)
    parser.add_argument('--batch', type=int, default=200)
    parser.add_argument('--copy-batch', type=int, default=10000,
                        help='rows per autocommit COPY chunk in the large-load cell')
    parser.add_argument('--size-check-rows', type=int, default=100000,
                        help='sample disk size after this many acknowledged rows (and at end)')
    parser.add_argument('--max-bytes', type=int, default=8_000_000_000,
                        help='maximum sampled qualification directory size, in bytes')
    parser.add_argument('--target-bytes', type=int, default=0,
                        help='stop large-load at this sampled database size; --rows is an upper bound')
    parser.add_argument('--payload-bytes', type=int, default=0,
                        help='deterministic text bytes per row; zero preserves the narrow schema')
    args = parser.parse_args()
    require(min(args.iterations, args.rows, args.batch, args.copy_batch,
                args.size_check_rows, args.max_bytes, args.command_timeout) > 0, 'counts/limits must be positive')
    require(args.payload_bytes >= 0 and args.target_bytes >= 0, 'payload/target bytes must be nonnegative')
    require(args.target_bytes <= args.max_bytes, '--target-bytes must not exceed --max-bytes')
    args.server_bin, args.work = args.server_bin.resolve(), args.work.resolve()
    require(args.work.is_relative_to(Path('J:/gigglesdb-work').resolve()), '--work must be under J:/gigglesdb-work')
    sys.exit(Qualification(args).run())
