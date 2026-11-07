"""Reproducible owned-source copy build and separate prepared-wire campaign."""
import argparse,os, collections, hashlib, json, pathlib, shutil, subprocess, sys
from conformance import ROOT,run,load_cases
from prepared_wire import WireReference
BASE=ROOT.parent.parent
from capture_provenance import OUT,save
from workdir import check_outside_repo
def execute(cmd,stem):
 check_outside_repo(OUT)
 p=run(cmd,timeout=1800);save(OUT/(stem+'.process.json'),dict(command=cmd,native_exit=p.returncode));(OUT/(stem+'.stdout')).write_text(p.stdout,encoding='utf-8');(OUT/(stem+'.stderr')).write_text(p.stderr,encoding='utf-8');return p

def fixtures():
 # PG Bind text is explicit; core Value representation is a separate contract.
 families=[('abs','abs($1)','-2.5',{'type':'double precision','value':'c004000000000000'}),('lower','lower($1)','ABC','ABC'),('length','length($1)','abc','abc'),('substring','substring($1,2,2)','abcd','abcd'),('binary','octet_length(CAST($1 AS bytea))','abc','abc'),('encode',"encode(CAST($1 AS bytea),'hex')",'abc','abc'),('numeric','abs(CAST($1 AS numeric))','-2.50',{'type':'numeric','value':'-2.50'}),('float','sqrt($1)','4',{'type':'double precision','value':'4010000000000000'}),('coalesce','coalesce($1, $2)','abc','abc')]
 cases=[]
 for name,expr,text,core in families:
  for null in [False,True]:
   cases.append(dict(id=name+('-null' if null else ''),query='SELECT '+expr+' AS v',wire=[None if null else text]+(['fallback'] if name=='coalesce' else []),core=[None if null else core]+(['fallback'] if name=='coalesce' else [])))
 cases += [dict(id='reuse',query='SELECT abs($1) AS a, abs($1) AS b',wire=['-2.5'],core=[{'type':'double precision','value':'c004000000000000'}]),dict(id='nested',query='SELECT length(lower($1)) AS v',wire=['ABC'],core=['ABC']),dict(id='cast-error',query='SELECT CAST($1 AS integer) AS v',wire=['bad'],core=['bad']),dict(id='context-order',setup=['CREATE TABLE f(i integer)'],query='SELECT abs($1) AS v FROM f WHERE i=$1',wire=['2.5'],core=[{'type':'double precision','value':'4004000000000000'}])]
 return cases

def main():
 # Source proof and execution share the same sealed campaign and executable.
 global OUT
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--output",type=pathlib.Path,default=OUT)
 OUT=parser.parse_args().output;check_outside_repo(OUT)
 import capture_provenance
 capture_provenance.OUT=OUT
 from run_provenance_campaign import main as campaign
 capture_provenance.build()
 return campaign()
if __name__=='__main__':raise SystemExit(main())
