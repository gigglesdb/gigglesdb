"""Owned, per-module unit-test storage; never removes prior task evidence."""
import os
from pathlib import Path
import shutil
import uuid

BASE = Path('J:/gigglesdb-work/r0-harness/unit-tmp')

class ModuleStorage:
    def __init__(self):
        self.root = BASE / uuid.uuid4().hex
        self.previous = None

    def start(self):
        self.root.mkdir(parents=True, exist_ok=False)
        self.previous = {key: os.environ.get(key) for key in ('TEMP', 'TMP', 'CARGO_TARGET_DIR')}
        os.environ.update(TEMP=str(self.root), TMP=str(self.root), CARGO_TARGET_DIR=str(self.root / 'target'))

    def finish(self):
        if self.previous is None:
            return
        for key, value in self.previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        target = self.root.resolve()
        if (self.root.is_symlink()
                or bool(getattr(self.root.lstat(), 'st_file_attributes', 0) & 0x400)
                or target != self.root.absolute()
                or target.parent != BASE.resolve() or len(target.name) != 32):
            raise ValueError('refusing cleanup outside owned module directory')
        shutil.rmtree(target)
        self.previous = None
