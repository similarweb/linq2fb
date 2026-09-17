#!/usr/bin/env python3
"""Rewrite linq2db dependency version in a packed nupkg nuspec."""
from __future__ import annotations

import re
import sys
import zipfile
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: patch_nupkg_range.py <nupkg> <version-range>", file=sys.stderr)
        return 2

    nupkg = Path(sys.argv[1])
    version_range = sys.argv[2]
    if not nupkg.is_file():
        print(f"nupkg not found: {nupkg}", file=sys.stderr)
        return 1

    pattern = re.compile(r'(<dependency id="linq2db" version=")[^"]+(")')
    replacement = rf"\g<1>{version_range}\g<2>"

    with zipfile.ZipFile(nupkg, "r") as zin:
        entries = {info.filename: zin.read(info.filename) for info in zin.infolist()}
        infos = list(zin.infolist())

    patched = False
    for name, data in list(entries.items()):
        if not name.endswith(".nuspec"):
            continue
        text = data.decode("utf-8")
        updated, count = pattern.subn(replacement, text)
        if count == 0:
            print(f"linq2db dependency not found in {name}", file=sys.stderr)
            return 1
        entries[name] = updated.encode("utf-8")
        patched = True

    if not patched:
        print(f"no nuspec in {nupkg}", file=sys.stderr)
        return 1

    tmp = nupkg.with_suffix(nupkg.suffix + ".tmp")
    with zipfile.ZipFile(tmp, "w") as zout:
        for info in infos:
            new_info = zipfile.ZipInfo(filename=info.filename, date_time=info.date_time)
            new_info.compress_type = info.compress_type
            new_info.external_attr = info.external_attr
            zout.writestr(new_info, entries[info.filename])
    tmp.replace(nupkg)
    print(f"Patched linq2db dependency in {nupkg.name} to {version_range}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
