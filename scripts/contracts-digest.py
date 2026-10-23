#!/usr/bin/env python3
"""Hash immutable schema/generated contract inputs, or verify a pinned tree lock."""
import argparse
import hashlib
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("contracts", type=Path)
parser.add_argument("--verify", type=Path)
args = parser.parse_args()
root = args.contracts.resolve()
files = sorted(path for folder in ("schema", "gen", "src") for path in (root / folder).rglob("*") if path.is_file())
if not files:
    raise SystemExit("contract source inventory is empty")
digest = hashlib.sha256()
for path in files:
    name = path.relative_to(root).as_posix().encode()
    content = path.read_bytes()
    digest.update(len(name).to_bytes(8, "big"))
    digest.update(name)
    digest.update(len(content).to_bytes(8, "big"))
    digest.update(content)
actual = digest.hexdigest()
if args.verify and args.verify.read_text(encoding="utf-8").strip() != actual:
    raise SystemExit("contracts tree digest differs from pinned source")
print(actual)
