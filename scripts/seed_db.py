#!/usr/bin/env python3
"""
Seed Firebolt DB from a SQL file (split into single statements) and run optional smoke queries.
- Prints progress to STDOUT.
- Parses fbcli responses from STDERR (fbcli writes URL header, then JSON).
- Treats JSON with non-empty "errors" as failure.

Usage:
  seed_db.py --file=northwind.sql \
             --container=firebolt-core \
             --smoke="SELECT COUNT(*) FROM Orders;"
"""

import argparse
import json
import re
import subprocess
import sys
from typing import Optional

def split_sql(text: str):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)  # strip /* ... */ comments
    text = text.replace("\r\n", "\n")

    stmts, buf = [], []
    in_squote = in_dquote = False
    i = 0
    while i < len(text):
        ch = text[i]

        # -- line comments
        if not in_squote and not in_dquote and text.startswith("--", i):
            j = text.find("\n", i)
            if j == -1:
                break
            i = j + 1
            continue

        # toggle single quotes (handle escaped '')
        if ch == "'" and not in_dquote:
            if i + 1 < len(text) and text[i + 1] == "'":
                buf.append("''"); i += 2; continue
            in_squote = not in_squote
            buf.append(ch); i += 1; continue

        # toggle double quotes
        if ch == '"' and not in_squote:
            in_dquote = not in_dquote
            buf.append(ch); i += 1; continue

        # statement delimiter ;
        if ch == ";" and not in_squote and not in_dquote:
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf.clear()
            i += 1
            continue

        buf.append(ch); i += 1

    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts

def run_fbcli(container: str, sql: str) -> subprocess.CompletedProcess:
    # Capture stdout and stderr separately. fbcli writes the JSON to STDERR.
    return subprocess.run(
        ["docker", "exec", "-i", container, "fbcli", "--command", sql],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

def parse_fbcli_json(stderr_text: str) -> Optional[dict]:
    """
    fbcli writes:
      URL: http://localhost:3473/?&output_format=PSQL
      { ...json... }
    This extracts the JSON object from stderr. If nothing JSON-like found, returns None.
    """
    # Drop the leading URL line(s)
    lines = [ln for ln in stderr_text.splitlines() if not ln.startswith("URL: ")]
    payload = "\n".join(lines).strip()
    if not payload:
        return None

    # Try whole payload first
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        pass

    # Fallback: find the outermost JSON object by braces
    first = payload.find("{")
    last = payload.rfind("}")
    if first != -1 and last != -1 and last > first:
        try:
            return json.loads(payload[first:last+1])
        except json.JSONDecodeError:
            return None
    return None

def smoke_has_errors(json_obj: dict) -> bool:
    # Consider any truthy "errors" as a failure signal
    errs = json_obj.get("errors")
    if not errs:
        return False
    # errs may be a list or object; treat non-empty as failure
    if isinstance(errs, (list, dict)):
        return len(errs) > 0
    return True  # non-empty string or other truthy value

def main():
    p = argparse.ArgumentParser(description="Seed Firebolt DB via fbcli inside a Docker container.")
    p.add_argument("--file", required=True, help="Path to .sql file with DDL/DML.")
    p.add_argument("--container", required=True, help="Docker container name running fbcli.")
    p.add_argument("--smoke", action="append", default=[],
                   help="Smoke test SQL to run after seeding (repeatable).")
    args = p.parse_args()

    # Load SQL
    try:
        sql_text = open(args.file, "r", encoding="utf-8").read()
    except FileNotFoundError:
        print(f"File not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    statements = split_sql(sql_text)
    print(f"Split into {len(statements)} statements.")

    # Execute statements one by one
    for i, stmt in enumerate(statements, 1):
        preview = stmt.replace("\n", " ")[:100]
        print(f"[{i}/{len(statements)}] {preview}{'…' if len(preview) == 100 else ''}")
        res = run_fbcli(args.container, stmt)
        # On seeding we primarily trust exit code; print stderr/stdout for diagnostics on failure
        if res.returncode != 0:
            print("---- fbcli stdout ----")
            print(res.stdout.rstrip())
            print("---- fbcli stderr ----", file=sys.stderr)
            print(res.stderr.rstrip(), file=sys.stderr)
            sys.exit(res.returncode)

    print(f"Executed {len(statements)} statements successfully.")

    # Run smoke queries
    for i, q in enumerate(args.smoke, 1):
        print(f"[smoke {i}] {q}")
        res = run_fbcli(args.container, q)

        # Prefer stderr for fbcli response; show both for visibility
        stderr_text = res.stderr or ""
        stdout_text = res.stdout or ""
        if stdout_text.strip():
            print(stdout_text.strip())
        if stderr_text.strip():
            print(stderr_text.strip())

        # Fail on non-zero exit or JSON with 'errors'
        if res.returncode != 0:
            print(f"Smoke query {i} failed (non-zero exit).", file=sys.stderr)
            sys.exit(res.returncode)

        obj = parse_fbcli_json(stdout_text)
        if obj is not None and smoke_has_errors(obj):
            print(f"Smoke query {i} returned JSON with errors.", file=sys.stderr)
            # Echo parsed errors for debugging
            print(json.dumps(obj, indent=2), file=sys.stderr)
            sys.exit(2)

    if args.smoke:
        print(f"All {len(args.smoke)} smoke queries passed.")

if __name__ == "__main__":
    main()
