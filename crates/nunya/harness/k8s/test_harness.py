"""Offline safety and wire-refusal tests; run with python -I test_harness.py."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent));sys.dont_write_bytecode=True
import unittest,struct
from unittest.mock import patch
import base_live as b
from wire import Portal,WireError
class Safety(unittest.TestCase):
 def run_network(self,exits,stderr=''):
  import scenarios as scenario
  from contextlib import ExitStack
  from subprocess import CompletedProcess
  pod={'metadata':{'name':'test'},'spec':{'nodeName':'node'},'status':{'podIP':'192.0.2.1'}}
  with ExitStack() as stack:
   stack.enter_context(patch.object(scenario,'pods',return_value=[pod,pod]))
   stack.enter_context(patch.object(scenario,'data',return_value={'items':[pod]}))
   stack.enter_context(patch.object(scenario,'command',side_effect=['sandbox','{"info":{"pid":1}}']*2))
   guard=stack.enter_context(patch.object(scenario,'guard'))
   probe=stack.enter_context(patch.object(scenario.subprocess,'run',side_effect=[CompletedProcess([],code,'',stderr) for code in exits]))
   evidence=stack.enter_context(patch.object(scenario,'ev'))
   try:return scenario.network()
   finally:
    self.assertEqual(probe.call_count,4);self.assertEqual(guard.call_count,4)
    self.network_evidence=evidence.call_args.args[1]
 def test_network_accepts_only_validated_denials(self):
  for blocked,stderr in [(124,''),(42,'NUNYA_CONNECTION_REFUSED\n')]:
   with self.subTest(blocked=blocked):
    result=self.run_network([blocked,blocked,0,blocked],stderr)
    self.assertTrue(result['non-nunya']['blocked'])
    self.assertTrue(all(x['admin_closed'] for x in result.values()))
 def test_network_execution_failures_never_qualify_as_denials(self):
  for code in [1,125,126,127,42]:
   for position in [0,1,3]:
    with self.subTest(code=code,position=position):
     exits=[124,124,0,124];exits[position]=code
     with self.assertRaises(AssertionError):self.run_network(exits,'nsenter: operation not permitted\n')
     evidence=self.network_evidence['nunya' if position==3 else 'non-nunya']
     probe=evidence if position==0 else evidence['admin_probe']
     self.assertFalse(probe['valid']);self.assertIn('operation not permitted',probe['stderr'])
 def test_refusal_marker_does_not_validate_an_execution_error(self):
  with self.assertRaises(AssertionError):self.run_network([1,124,0,124],'NUNYA_CONNECTION_REFUSED\n')
 def test_wrong_context_blocks_every_mutation(self):
  for verb in ['apply','delete','create','replace','patch','exec']:
   with self.subTest(verb=verb),patch.object(b,'command',return_value='production') as command:
    with self.assertRaises(AssertionError):b.kub([verb,'anything'],'must-not-run')
    self.assertEqual(command.call_count,1)
    self.assertEqual(command.call_args.args[0],[b.K,'--kubeconfig',b.env['KUBECONFIG'],'config','current-context'])
 def test_read_always_explicit_kubeconfig(self):
  with patch.object(b,'command',return_value='{}') as command:
   b.kub(['get','pods'],'read');self.assertEqual(command.call_args.args[0][1:3],['--kubeconfig',b.env['KUBECONFIG']])
 def test_pre_tls_NY013_is_unavailable(self):
  payload=b'SERROR\0CNY013\0Munavailable\0\0'
  class Socket:
   def sendall(self,v):pass
   def recv(self,n):
    v=self.data[:n];self.data=self.data[n:];return v
   def close(self):self.closed=True
  sock=Socket();sock.data=b'E'+struct.pack('!I',len(payload)+4)+payload;sock.closed=False
  with patch('wire.socket.create_connection',return_value=sock):
   with self.assertRaises(WireError) as error:Portal(1234,'test')
  self.assertEqual(error.exception.sqlstate,'NY013');self.assertTrue(sock.closed)
 def test_failed_budget_blocks_takeover_but_continues_independent_tests(self):
  import scenarios as scenario
  from contextlib import ExitStack
  names=['stable','shared','revoke','limits','kill','tighten','partition','restart','network']
  with ExitStack() as stack:
   for name in names:
    mock=stack.enter_context(patch.object(scenario,name,side_effect=RuntimeError('budget failed') if name=='limits' else None,return_value={}))
    mock.__name__=name
   stack.enter_context(patch.object(scenario,'ev'))
   stack.enter_context(patch('builtins.print'))
   stack.enter_context(patch('pathlib.Path.write_text'))
   stack.enter_context(patch.object(b,'results',{}))
   scenario.run_scenarios()
   self.assertEqual(b.results['3']['status'],'FAIL')
   self.assertEqual(b.results['4']['status'],'BLOCKED')
   for n in [0,1,2,5,6,7,8]:self.assertEqual(b.results[str(n)]['status'],'PASS')
 def test_kubectl_json_sequence(self):
  import render,json
  objects=[{'apiVersion':'v1','kind':kind} for kind in ['ConfigMap','PersistentVolumeClaim','Deployment','Service']]
  self.assertEqual(render.json_documents('\n'.join(json.dumps(v) for v in objects))['items'],objects)
  self.assertEqual(render.json_documents(json.dumps({'kind':'List','items':objects}))['items'],objects)
 def test_fatal_refusal_does_not_require_ready_for_query(self):
  for code in ['NY011','NY013']:
   portal=Portal.__new__(Portal)
   portal.recv=lambda:(b'E',b'SFATAL\0C'+code.encode()+b'\0Munavailable\0\0')
   with self.assertRaises(WireError) as error:portal.drain()
   self.assertEqual(error.exception.sqlstate,code)
if __name__=='__main__':unittest.main()
