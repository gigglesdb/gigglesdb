"""Adapt S1's TLS setup and S4's strictly run-scoped lifecycle."""
import pathlib
ROOT=pathlib.Path(__file__).resolve().parent
s=(ROOT.parent/'s1-replication/run.py').read_text()
s=s.replace('s1-pg%d-%d', 's2-pg%d-%d').replace("('s1-'+RUN)", "('s2-'+RUN)")
start=s.index("    h.save('toxi-create.json'")
stop=s.index('    env=os.environ.copy()',start)
s=s[:start]+s[stop:]
s=s.replace("S1_TOXI_PORT=str(port('toxiproxy',5433)), S1_TOXI_API=str(port('toxiproxy',8474)), ", "S2_SESSION_PORT=str(port('poolsession',5432)), S2_TRANSACTION_PORT=str(port('pgbouncer',5432)), S2_PLAIN_PORTS=str(port('poolsession',5432))+','+str(port('pgbouncer',5432)), ")
s=s.replace('target/debug/s1-replication.exe', 'target/s2/debug/s2-fencing.exe')
s=s.replace("wal_sender_timeout='4s'", "wal_sender_timeout='60s'")
s=s.replace('timeout=360', 'timeout=480')
s=s.replace('    env=os.environ.copy()', "    h.save('pool-session-config.json',h.execute('poolsession','cat','/etc/pgbouncer/pgbouncer.ini'))\n    h.save('pool-transaction-config.json',h.execute('pgbouncer','cat','/etc/pgbouncer/pgbouncer.ini'))\n    env=os.environ.copy()")
s=s.replace('        try: h.check(RUN)', "        try: h.save('leftovers.json',h.resources(RUN)); h.check(RUN)")
(ROOT/'run.py').write_text(s)
