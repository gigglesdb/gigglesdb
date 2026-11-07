"""Output policy tests; no artifacts or reference containers are created."""
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import workdir


class WorkdirTests(unittest.TestCase):
    def test_default_root(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(workdir.work_root(), Path("J:/gigglesdb-work"))

    def test_override(self):
        with patch.dict(os.environ, {"GIGGLESDB_WORK_ROOT": "J:/gigglesdb-work/custom"}):
            self.assertEqual(workdir.work_root(), Path("J:/gigglesdb-work/custom"))

    def test_default_name_without_creation(self):
        with patch.dict(os.environ, {"GIGGLESDB_WORK_ROOT": "J:/gigglesdb-work/nonexistent-workdir-test"}):
            with patch.object(Path, "mkdir", side_effect=AssertionError("must not create")):
                output = workdir.default_output("sample")
        self.assertEqual(output.parent, Path("J:/gigglesdb-work/nonexistent-workdir-test"))
        self.assertRegex(output.name, rf"^dd68-pgfunc-sample-\d{{8}}T\d{{6}}Z-{os.getpid()}$")

    def test_reject_repository_and_resolved_paths(self):
        repo = Path(workdir.__file__).resolve().parents[1].parent
        for output in [repo, repo / "new", repo / "scripts" / ".." / "new"]:
            with self.subTest(output=output), self.assertRaisesRegex(SystemExit, "outside repository"):
                workdir.check_outside_repo(output)
        with patch.dict(os.environ, {"GIGGLESDB_WORK_ROOT": str(repo)}):
            with self.assertRaises(SystemExit):
                workdir.check_outside_repo(workdir.default_output("bad"))

    def test_accept_outside(self):
        repo = Path(workdir.__file__).resolve().parents[1].parent
        workdir.check_outside_repo(repo.parent / "outside-workdir-test")


if __name__ == "__main__":
    unittest.main()
