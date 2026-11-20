"""Normalize historical checkout paths in JSONPath provenance manifests.

Run with --source-root pointing to the checkout used by the recorded commands.
Hashes in these records describe historical verification inputs, not this
normalization; preserve them along with external evidence paths.
"""
import argparse
import json
from pathlib import Path


def normalize(value, source_root):
    if isinstance(value, str):
        path = value.replace("\\", "/")
        root = source_root.replace("\\", "/").rstrip("/")
        if path == root:
            return "."
        if path.startswith(root + "/"):
            return path[len(root) + 1:]
        return value
    if isinstance(value, list):
        return [normalize(item, source_root) for item in value]
    if isinstance(value, dict):
        return {normalize(key, source_root): normalize(item, source_root)
                for key, item in value.items()}
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    args = parser.parse_args()
    fixtures = Path(__file__).parent / "fixtures"
    for name in ("continuations", "methods", "selectors"):
        path = fixtures / (name + "-manifest.json")
        record = normalize(json.loads(path.read_text(encoding="utf-8")), args.source_root)
        path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8", newline="\n")
