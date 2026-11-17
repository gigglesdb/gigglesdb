"""Generate the Phase 12 cluster manifest, mTLS PKI and server configs."""
import argparse
import copy
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import ssl
import subprocess

OPENSSL_CNF = """[req]
distinguished_name = dn
prompt = no
[dn]
CN = gigglesdb-compat-test-ca
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
[v3_leaf]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = DNS:localhost
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
"""


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8', newline='\n')


def split_keys():
    # range_api.rs: range_key = tag || user_key; Rows=0x80, Index=0x90.
    return [(bytes([tag]) + n.to_bytes(8, 'big')).hex()
            for tag in (0x80, 0x90) for n in range(1, 9)]


def openssl_commands(nodes):
    commands = [
        ['genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256', '-out', 'ca.key'],
        ['req', '-x509', '-new', '-config', 'openssl.cnf', '-extensions', 'v3_ca', '-key', 'ca.key',
         '-sha256', '-days', '30', '-out', 'ca.pem']]
    for n in range(1, nodes + 1):
        commands += [
            ['genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256', '-out', f'node{n}.key'],
            ['req', '-new', '-config', 'openssl.cnf', '-key', f'node{n}.key',
             '-subj', f'/CN=gigglesdb-compat-node{n}', '-out', f'node{n}.csr'],
            ['x509', '-req', '-in', f'node{n}.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key',
             '-set_serial', str(int.from_bytes(secrets.token_bytes(8), 'big')), '-days', '30', '-sha256',
             '-extfile', 'openssl.cnf', '-extensions', 'v3_leaf', '-out', f'node{n}.pem']]
    return commands


def generate_pki(directory, nodes):
    binary = shutil.which('openssl') or next((str(p) for p in (
        Path('C:/Program Files/Git/mingw64/bin/openssl.exe'),
        Path('C:/Program Files/Git/usr/bin/openssl.exe')) if p.is_file()), None)
    if not binary:
        raise RuntimeError('OpenSSL CLI not found')
    directory.mkdir(parents=True)
    (directory / 'openssl.cnf').write_text(OPENSSL_CNF, encoding='utf-8', newline='\n')
    for args in openssl_commands(nodes):
        subprocess.run([binary, *args], cwd=directory, check=True, capture_output=True, timeout=60)
    return [hashlib.sha256(ssl.PEM_cert_to_DER_cert(
        (directory / f'node{n}.pem').read_text())).hexdigest() for n in range(1, nodes + 1)]


def configs(args, fingerprints):
    root = Path(args.root).resolve()
    local = args.local
    hosts = ['127.0.0.1'] * args.nodes if local else args.hosts.split(',')
    if not 3 <= args.nodes <= 6 or len(hosts) != args.nodes:
        raise ValueError('require 3..6 nodes and exactly one host per node')
    if not local:
        hosts = [str(ipaddress.IPv6Address(h.strip().strip('[]'))) for h in hosts]
    nodes = [dict(id=k, host=h, pg_port=args.pg_port + (k if local else 0),
                  peer_port=args.peer_port + (k if local else 0),
                  agent_port=args.agent_port + (k if local else 0), dir=str(root / f'node-{k}'))
             for k, h in enumerate(hosts, 1)]
    optional = {key: getattr(args, key) for key in (
        'split_size_bytes', 'split_interval_ms', 'split_cooldown_ms') if getattr(args, key) is not None}
    manifest = dict(version=1, mode_default='ranges', owner='owner', password=secrets.token_hex(24),
                    database='postgres', binary=str(Path(args.binary).resolve()), root=str(root), nodes=nodes,
                    modes=dict(ranges=dict(split_tables=8, split_size_bytes=args.split_size_bytes),
                               single_range={}, replication={}))
    cluster_id, result = secrets.token_hex(16), {}
    for node in nodes:
        k, directory = node['id'], Path(node['dir'])
        def address(n, port, listen=False):
            return f"127.0.0.1:{n[port]}" if local else f"[{'::' if listen else n['host']}]:{n[port]}"
        common = dict(database=str(directory / 'db'), native='127.0.0.1:0', postgres='127.0.0.1:0',
                      certificate=str(root / 'pki' / f'node{k}.pem'),
                      private_key=str(root / 'pki' / f'node{k}.key'), client_ca=str(root / 'pki' / 'ca.pem'),
                      bindings=[], connections=args.connections, skew_seconds=0, max_pause_seconds=3600,
                      postgresql=dict(listen=address(node, 'pg_port', True), tls='disabled', loopback_trust=True))
        section = dict(node_id=k, cluster=cluster_id, listen=address(node, 'peer_port'),
                       log_dir=str(directory / 'raftlog'), tick_ms=args.tick_ms, owner='owner',
                       peers=[dict(id=n['id'], address=address(n, 'peer_port'),
                                   fingerprint=fingerprints[n['id'] - 1]) for n in nodes if n != node])
        for mode in manifest['modes']:
            config, group = copy.deepcopy(common), copy.deepcopy(section)
            if mode == 'replication':
                config['postgresql'].update(loopback_trust=False, databases=['postgres'])
            else:
                group.update(initial_members=list(range(1, args.nodes + 1)), **optional)
                if mode == 'ranges':
                    group['split_keys'] = split_keys()
            config['replication' if mode == 'replication' else 'cluster'] = group
            result[directory / f'{mode}.json'] = config
    return manifest, result


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=['init'])
    p.add_argument('--nodes', type=int, required=True, choices=range(3, 7))
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--local', action='store_true')
    group.add_argument('--hosts')
    p.add_argument('--root', required=True)
    p.add_argument('--binary', required=True)
    for name, default in [('pg-port', 15432), ('peer-port', 17400), ('agent-port', 17999),
                          ('tick-ms', 100), ('connections', 128), ('split-size-bytes', None),
                          ('split-interval-ms', None), ('split-cooldown-ms', None)]:
        p.add_argument('--' + name, type=int, default=default)
    return p


def initialize(args):
    # Validate before creating any files; never overwrite a previous run.
    configs(args, ['0' * 64] * args.nodes)
    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    manifest, files = configs(args, generate_pki(root / 'pki', args.nodes))
    for path, config in files.items():
        path.parent.mkdir(exist_ok=True)
        write_json(path, config)
    write_json(root / 'cluster.json', manifest)
    os.chmod(root / 'cluster.json', 0o600)
    return root / 'cluster.json'


if __name__ == '__main__':
    print(initialize(parser().parse_args()))
