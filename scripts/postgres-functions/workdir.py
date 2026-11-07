"""Output locations for DD-0068; existing repository evidence is read-only."""
import os
from datetime import datetime, timezone
from pathlib import Path


def work_root():
    return Path(os.environ.get("GIGGLESDB_WORK_ROOT", "J:/gigglesdb-work"))


def default_output(name):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return work_root() / f"dd68-pgfunc-{name}-{stamp}-{os.getpid()}"


def check_outside_repo(path):
    resolved = Path(path).resolve()
    repo = Path(__file__).resolve().parents[1].parent
    if resolved == repo or repo in resolved.parents:
        raise SystemExit(f"Output must be outside repository {repo}: {resolved}")
