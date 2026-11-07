import json
from pathlib import Path

def load():
    return json.loads((Path(__file__).resolve().parent.parent/'pins.json').read_text(encoding='utf-8'))

def reference(pin):
    return pin['image'].split(':')[0] + '@' + pin['digest']
