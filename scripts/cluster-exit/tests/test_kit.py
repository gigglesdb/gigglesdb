"""Config oracles and one opt-in, real three-process cluster lifecycle test."""
import ipaddress
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kit
from agent import Agent
from lib import Cluster, wait_for

ITEM = Path('J:/gigglesdb-work/r5/p12/w1-kit') if os.name == 'nt' else Path(
    os.environ.get('CX_RUN_ROOT', '/tmp/cluster-exit'))


def generated(tmp_path, *flags):
    args = kit.parser().parse_args(['init', '--nodes', '3', '--root', str(tmp_path / 'cluster'),
                                   '--binary', str(tmp_path / 'server'), *flags])
    return kit.configs(args, [str(n) * 64 for n in range(1, 4)])


def test_split_key_oracle():
    keys = kit.split_keys()
    assert len(keys) == 16 and keys == sorted(keys)
    assert keys[0] == '800000000000000001'
    assert keys[-1] == '900000000000000008'


def test_local_ports_and_shape(tmp_path):
    manifest, files = generated(tmp_path, '--local', '--connections', '71', '--tick-ms', '150')
    assert manifest['version'] == 1 and manifest['owner'] == 'owner'
    assert len(manifest['password']) == 48
    assert len(files) == 9
    for node in manifest['nodes']:
        k = node['id']
        assert (node['pg_port'], node['peer_port'], node['agent_port']) == (15432+k, 17400+k, 17999+k)
        assert node['host'] == '127.0.0.1'
        config = files[Path(node['dir']) / 'ranges.json']
        assert config['postgresql'] == dict(listen=f'127.0.0.1:{15432+k}', tls='disabled', loopback_trust=True)
        assert config['connections'] == 71
        assert config['cluster']['tick_ms'] == 150
        assert config['cluster']['initial_members'] == [1, 2, 3]
        assert config['cluster']['split_keys'] == kit.split_keys()
        assert len(config['cluster']['peers']) == 2
        for field in ('database', 'certificate', 'private_key', 'client_ca'):
            assert Path(config[field]).is_absolute()
        assert Path(config['cluster']['log_dir']).is_absolute()


def test_remote_ipv6(tmp_path):
    manifest, files = generated(tmp_path, '--hosts', '2001:db8::1,2001:db8::2,2001:db8::3')
    for node in manifest['nodes']:
        assert (node['pg_port'], node['peer_port'], node['agent_port']) == (15432, 17400, 17999)
        for mode in ('ranges', 'single_range', 'replication'):
            config = files[Path(node['dir']) / f'{mode}.json']
            group = config['replication' if mode == 'replication' else 'cluster']
            assert config['postgresql']['listen'] == '[::]:15432'
            assert group['listen'] == f"[{node['host']}]:17400"
            assert [p['address'] for p in group['peers']] == [
                f"[{n['host']}]:17400" for n in manifest['nodes'] if n != node]


def test_replication_shape(tmp_path):
    _, files = generated(tmp_path, '--local')
    configs = [c for path, c in files.items() if path.name == 'replication.json']
    for config in configs:
        assert 'cluster' not in config
        assert set(config['replication']) == {'node_id', 'cluster', 'listen', 'log_dir', 'tick_ms', 'owner', 'peers'}
        assert config['postgresql']['loopback_trust'] is False
        assert config['postgresql']['databases'] == ['postgres']
    assert len({c['replication']['cluster'] for c in configs}) == 1


def test_stopped_agent_endpoints(tmp_path):
    manifest, _ = generated(tmp_path, '--local')
    agent = Agent(manifest, 1)
    agent.directory.mkdir(parents=True)
    for name in ('db', 'raftlog', 'raftlog.snapshots'):
        (agent.directory / name).mkdir()
    assert agent.request('GET', '/status', {})[1]['pid'] is None
    assert agent.request('POST', '/kill', {})[1]['ended'] == 'already_stopped'
    assert agent.request('POST', '/wipe', {}) == (200, {'wiped': True})
    assert not (agent.directory / 'db').exists()
    assert agent.request('GET', '/log?tail=1', {})[1]['lines'] == []
    (agent.directory / 'stdout.log').write_text('first\nsecond\n', newline='\n')
    assert agent.request('GET', '/log?tail=1', {})[1]['lines'] == ['second\n']
    assert agent.request('GET', '/unknown', {})[0] == 404
    with pytest.raises(ValueError):
        agent.request('POST', '/start', {'mode': '../invalid'})
    if os.name == 'nt':
        assert agent.request('POST', '/isolate', {'peers': 'all'})[0] == 501


@pytest.mark.parametrize('flags', [[], ['--split-size-bytes', '4096', '--split-interval-ms', '500',
                                     '--split-cooldown-ms', '1000']])
def test_split_flags(tmp_path, flags):
    _, files = generated(tmp_path, '--local', *flags)
    for path, config in files.items():
        group = config.get('cluster', config.get('replication'))
        for key, expected in [('split_size_bytes', 4096), ('split_interval_ms', 500), ('split_cooldown_ms', 1000)]:
            assert group.get(key) == (expected if flags and path.name != 'replication.json' else None)
            if not flags or path.name == 'replication.json':
                assert key not in group
        if path.name == 'single_range.json':
            assert 'split_keys' not in group


@pytest.mark.skipif(not os.environ.get('CX_BINARY'), reason='set CX_BINARY for real cluster test')
def test_real_cluster():
    import psycopg
    ITEM.mkdir(parents=True, exist_ok=True)
    root = ITEM / f'run-{uuid.uuid4().hex}'
    # Reserve disjoint blocks while selecting ports, then release before starting agents.
    held, bases = [], []
    for _ in range(3):
        while True:
            probe = socket.socket(); probe.bind(('127.0.0.1', 0))
            base = probe.getsockname()[1] - 1
            block = [probe]
            try:
                for offset in (2, 3):
                    sock = socket.socket(); block.append(sock); sock.bind(('127.0.0.1', base + offset))
                held.extend(block); bases.append(base); break
            except OSError:
                for sock in block: sock.close()
    args = kit.parser().parse_args(['init', '--nodes', '3', '--local', '--root', str(root),
        '--binary', os.environ['CX_BINARY'], '--pg-port', str(bases[0]), '--peer-port', str(bases[1]),
        '--agent-port', str(bases[2])])
    processes, logs, cluster = [], [], None
    try:
        manifest = kit.initialize(args)
        # Hand-edited run copy exposes pg only, exercising SCRAM over a non-loopback peer.
        config_path = root / 'node-1' / 'ranges.json'
        config = json.loads(config_path.read_text())
        config['postgresql']['listen'] = f'0.0.0.0:{bases[0]+1}'
        kit.write_json(config_path, config)
        for sock in held: sock.close()
        cluster = Cluster.load(manifest)
        for n in cluster.nodes:
            log = (root / f"agent-{n['id']}.log").open('w')
            logs.append(log)
            processes.append(subprocess.Popen([sys.executable, str(Path(kit.__file__).with_name('agent.py')),
                '--cluster', str(manifest), '--node', str(n['id'])], stdout=log, stderr=log,
                start_new_session=os.name != 'nt'))
        for n in cluster.nodes:
            wait_for(lambda: cluster.agent(n['id']).status(), 15)
        cluster.reset('ranges')
        for n in cluster.nodes:
            with cluster.connect(n['id']) as c: assert c.execute('SELECT 1').fetchone() == (1,)
        lan = next(addr[4][0] for addr in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
                   if not ipaddress.ip_address(addr[4][0]).is_loopback)
        with cluster.connect(1, host=lan) as c: assert c.execute('SELECT 1').fetchone() == (1,)
        with pytest.raises(psycopg.OperationalError):
            cluster.connect(1, host=lan, password='incorrect-password').close()
        print(f'non-loopback SCRAM: {lan}, correct password accepted, wrong password rejected')
        with cluster.connect(1) as c:
            c.execute('CREATE TABLE kit_probe (id integer PRIMARY KEY)'); c.execute('INSERT INTO kit_probe VALUES (1)')
        with cluster.connect(3) as c: assert c.execute('SELECT id FROM kit_probe').fetchall() == [(1,)]
        cluster.agent(2).kill()
        def query_one():
            with cluster.connect(1, connect_timeout=2) as c: return c.execute('SELECT 1').fetchone() == (1,)
        wait_for(query_one, 30)
        cluster.agent(2).start('ranges')
        wait_for(lambda: cluster.agent(2).status()['ready'], 90)
        assert cluster.agent(1).admin(['cluster', 'status', '--json'])['rc'] == 0
        cluster.reset('single_range')
        cluster.reset('replication')
        leader = wait_for(cluster.leader, 30)
        with cluster.connect(leader) as c:
            c.execute('CREATE TABLE kit_repl (id integer PRIMARY KEY)'); c.execute('INSERT INTO kit_repl VALUES (1)')
            assert c.execute('SELECT id FROM kit_repl').fetchall() == [(1,)]
    except Exception:
        for log in root.rglob('*.log'):
            print(f'{log.name}: {log.read_text(errors="replace")[-2000:]}')
        raise
    finally:
        for sock in held: sock.close()
        if cluster:
            for n in cluster.nodes:
                try: cluster.agent(n['id']).stop()
                except Exception: pass
        for p in processes:
            if p.poll() is None:
                if os.name == 'nt':
                    subprocess.run(['taskkill', '/T', '/F', '/PID', str(p.pid)], capture_output=True, check=False)
                else:
                    os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=20)
        for log in logs: log.close()
        if root.exists():
            shutil.rmtree(root)
