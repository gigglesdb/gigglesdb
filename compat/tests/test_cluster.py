import base64
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compat_lib import cluster
from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
def setUpModule():
    _STORAGE.start()
def tearDownModule():
    _STORAGE.finish()


class ClusterTests(unittest.TestCase):
    def test_role_lines(self):
        leader = cluster.role_line('native=127.0.0.1:1 postgres=127.0.0.1:2 postgresql=127.0.0.1:3 preview=replicated role=leader term=4 node=2\n')
        self.assertEqual((leader['role'], leader['term'], leader['node'], leader['postgresql']), ('leader', '4', '2', '127.0.0.1:3'))
        self.assertEqual(cluster.role_line('postgres=127.0.0.1:2 preview=replicated role=follower node=1')['role'], 'follower')
        for other in ('postgresql=127.0.0.1:3 preview=single-host', 'bindings reloaded epoch=2', ''):
            self.assertIsNone(cluster.role_line(other))

    def test_unique_leader_waits_for_every_live_node(self):
        follower, leader = {'role': 'follower'}, {'role': 'leader'}
        self.assertIsNone(cluster.unique_leader([leader, follower, None], [True]*3))
        self.assertEqual(cluster.unique_leader([follower, leader, follower], [True]*3), 1)
        self.assertIsNone(cluster.unique_leader([leader, leader, follower], [True]*3))
        self.assertEqual(cluster.unique_leader([leader, follower, None], [True, True, False]), 0)

    def test_der_fingerprint(self):
        der = bytes(range(200))
        body = base64.b64encode(der).decode()
        pem = '-----BEGIN CERTIFICATE-----\n' + '\n'.join(body[i:i+64] for i in range(0, len(body), 64)) + '\n-----END CERTIFICATE-----\n'
        self.assertEqual(cluster.der_fingerprint(pem), hashlib.sha256(der).hexdigest())
        with self.assertRaises(ValueError):
            cluster.der_fingerprint('no certificate')

    def test_node_configs(self):
        root, pki = Path(_STORAGE.root)/'c', Path(_STORAGE.root)/'c'/'pki'
        ports = [(10, 11), (20, 21), (30, 31)]
        configs = cluster.node_configs(root, pki, ports, ['a'*64, 'b'*64, 'c'*64], '0'*32)
        self.assertEqual(len(configs), 3)
        for i, config in enumerate(configs):
            replication = config['replication']
            self.assertEqual(replication['node_id'], i + 1)
            self.assertEqual(replication['owner'], 'postgres')
            self.assertEqual(sorted(peer['id'] for peer in replication['peers']), [j + 1 for j in range(3) if j != i])
            self.assertNotIn(replication['listen'], [peer['address'] for peer in replication['peers']])
            self.assertEqual(config['postgresql']['listen'], f'127.0.0.1:{ports[i][0]}')
            self.assertFalse(config['postgresql']['loopback_trust'])
            self.assertEqual(config['postgresql']['databases'], ['postgres'])
            self.assertFalse(Path(replication['log_dir']).is_relative_to(Path(config['database'])))
            self.assertEqual(config['database'], str(root/f'node-{i + 1}'/'db'))

    def test_openssl_plan_outputs(self):
        commands = cluster.openssl_commands(3)
        written = {args[args.index('-out') + 1] for args in commands}
        self.assertTrue(set(cluster.output_files(3)) <= written)
        self.assertIn('subjectAltName = DNS:localhost', cluster.OPENSSL_CNF)

    def test_rejects_dev_server_args(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as directory:
            with self.assertRaises(ValueError):
                cluster.ClusterServer(Path(__file__), Path(directory), {}, 'pw', server_args=['--engine'])

    def test_cluster_flag(self):
        import harness
        self.assertTrue(harness.parser().parse_args(['run', '--cluster']).cluster)
        self.assertFalse(harness.parser().parse_args(['run']).cluster)


if __name__ == '__main__':
    unittest.main()
