#!/usr/bin/env python3
"""Generate macOS IntelliJ runs from their Windows counterparts (stdlib only)."""

import argparse
import html
from pathlib import Path
import re
import shlex
import sys
import xml.etree.ElementTree as ET


SCRIPTS = {
    "$PROJECT_DIR$/scripts/dev.ps1": "$PROJECT_DIR$/scripts/dev.sh",
    "$PROJECT_DIR$/scripts/check-pg-clients.ps1": "$PROJECT_DIR$/scripts/check-pg-clients.sh",
}
OPTIONS = {
    "-Task": "--task", "-Crate": "--crate", "-Release": "--release",
    "-Work": "--work", "-Port": "--port", "-JavaHome": "--java-home",
    "-JdbcJar": "--jdbc-jar",
}
OPTIONS.update({option: option for option in list(OPTIONS.values())})


def translate_options(value):
    tokens = shlex.split(value)
    result = []
    index = 0
    while index < len(tokens):
        option = tokens[index]
        if option not in OPTIONS:
            raise ValueError(f"unknown SCRIPT_OPTIONS option: {option!r}")
        result.append(OPTIONS[option])
        index += 1
        if OPTIONS[option] != "--release":
            if index == len(tokens) or tokens[index].startswith("-"):
                raise ValueError(f"missing value for {option}")
            result.append(tokens[index])
            index += 1
    return shlex.join(result)


def replace_attribute(tag, attribute, value):
    pattern = rf'(\b{re.escape(attribute)}=")[^"]*(")'
    replacement = html.escape(value, quote=True)
    tag, count = re.subn(pattern, lambda m: m[1] + replacement + m[2], tag)
    if count != 1:
        raise ValueError(f"expected exactly one {attribute} attribute")
    return tag


def generate(text, configuration, options):
    changes = {
        "SCRIPT_PATH": SCRIPTS[options["SCRIPT_PATH"]],
        "SCRIPT_OPTIONS": translate_options(options["SCRIPT_OPTIONS"]),
        "INTERPRETER_PATH": "/bin/bash",
        "INTERPRETER_OPTIONS": "",
    }
    def configuration_tag(match):
        tag = replace_attribute(match[0], "name", configuration.attrib["name"] + " (macOS)")
        return replace_attribute(tag, "folderName", "macOS - " + configuration.attrib["folderName"])
    text, count = re.subn(r'<configuration\b[^>]*>', configuration_tag, text)
    if count != 1:
        raise ValueError("expected exactly one configuration")
    for name, value in changes.items():
        pattern = rf'<option\b(?=[^>]*\bname="{name}")[^>]*>'
        text, count = re.subn(pattern, lambda m: replace_attribute(m[0], "value", value), text)
        if count != 1:
            raise ValueError(f"expected exactly one {name} option")
    return text.encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="report missing, stale or orphan macOS runs without writing")
    args = parser.parse_args()
    run_dir = Path(__file__).resolve().parent.parent / ".run"
    expected = {}
    for source in sorted(run_dir.glob("*.run.xml")):
        if source.name.startswith("macOS_"):
            continue
        try:
            text = source.read_text(encoding="utf-8")
            configuration = ET.fromstring(text).find("configuration")
            if configuration is None:
                continue
            options = {node.attrib["name"]: node.attrib.get("value", "") for node in configuration.findall("option")}
            if options.get("SCRIPT_PATH") not in SCRIPTS:
                continue
            expected[run_dir / ("macOS_" + source.name)] = generate(text, configuration, options)
        except (ValueError, KeyError, ET.ParseError) as error:
            parser.error(f"{source.name}: {error}")
    orphans = sorted(set(run_dir.glob("macOS_*.run.xml")) - expected.keys())
    if args.check:
        problems = []
        for path, content in expected.items():
            if not path.exists():
                problems.append(f"missing: {path.name}")
            elif path.read_bytes() != content:
                problems.append(f"stale: {path.name}")
        problems.extend(f"orphan: {path.name}" for path in orphans)
        if problems:
            print("\n".join(problems), file=sys.stderr)
            return 1
        print(f"Checked {len(expected)} macOS run configurations: current")
        return 0
    for path, content in expected.items():
        path.write_bytes(content)
    for path in orphans:
        print(f"orphan (retained): {path.name}", file=sys.stderr)
    print(f"Generated {len(expected)} macOS run configurations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
