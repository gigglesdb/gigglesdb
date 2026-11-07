"""Reobserve prepared fixture family; never rewrite legacy fixtures or expectations."""
import argparse,json
from pathlib import Path
from workdir import check_outside_repo
from conformance import ROOT,digest,IMAGE
from prepared_wire import WireReference

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--output',type=str,required=True);args=parser.parse_args()
 output=Path(args.output);check_outside_repo(output);output.parent.mkdir(parents=True,exist_ok=True)
 cases=json.loads((ROOT/'cases-prepared.json').read_text(encoding='utf-8'));pin=json.loads((ROOT/'prepared-reference.json').read_text(encoding='utf-8'))
 assert pin['corpus_sha256']==digest(cases) and pin['image']==IMAGE
 results=[]
 with WireReference() as ref:
  for c in cases:
   actual,trace=ref.observe(c['query'],c['wire'],c.get('setup',[]));results.append(dict(id=c['id'],matches=actual==c['expected_pg'],actual=actual,trace=trace))
 with open(args.output,'x',encoding='utf-8') as f:json.dump(results,f,indent=2)
 return int(not all(c['matches'] for c in results))
if __name__=='__main__':raise SystemExit(main())
