"""Conservative deletion of explicitly named, owned scratch directories."""
import os
from pathlib import Path
import shutil

RUN_CHILDREN = ('bin', 'data', 'tmp', 'target', 'cwd')


def _redirect(path):
    return path.is_symlink() or bool(getattr(path.lstat(), 'st_file_attributes', 0) & 0x400)


def remove_children(parent, names=RUN_CHILDREN, *, keep=False, _allowed=RUN_CHILDREN):
    """Validate the entire deletion set before removing any of it.

    The caller owns parent and must stop all processes using it first. No glob,
    arbitrary path, link, junction or nested reparse point is accepted.
    """
    parent = Path(parent).absolute()
    if parent.resolve(strict=True) != parent or _redirect(parent):
        raise ValueError('refusing redirected scratch parent')
    if len(set(names)) != len(names) or any(name not in _allowed for name in names):
        raise ValueError('invalid scratch child name')
    candidates = []
    for name in names:
        child = parent/name
        if not child.exists() and not child.is_symlink():
            continue
        if _redirect(child) or child.resolve() != parent/name or not child.is_dir():
            raise ValueError('refusing redirected scratch child')
        size = 0
        for root, dirs, files in os.walk(child, followlinks=False):
            for entry in dirs + files:
                path = Path(root)/entry
                if _redirect(path):
                    raise ValueError('refusing nested scratch redirect')
                if entry in files:
                    size += path.stat().st_size
        candidates.append((child, size))
    removed, freed = [], 0
    if not keep:
        for child, size in candidates:
            shutil.rmtree(child)
            removed.append(child.name)
            freed += size
    return dict(scratch_removed=removed, scratch_bytes_freed=freed)
