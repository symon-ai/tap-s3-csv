#!/usr/bin/env python3
"""Reproduce Sales Planning CSV physical-line limits via tap_s3_csv.dialect.detect_dialect."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from tap_s3_csv import dialect  # noqa: E402


def run_detect(csv_path: Path, allow_2mb: bool) -> None:
    data = csv_path.read_bytes()
    config = {"bucket": "local-test-bucket", "allow_2mb_csv_lines": allow_2mb}
    table = {
        "table_name": "Rule Based Assignment | Territory Rules",
        "search_prefix": "",
        "search_pattern": ".*",
    }
    s3_file = {"key": csv_path.name}

    class S3LikeBody:
        def __init__(self, raw: bytes):
            self._raw = raw

        def iter_lines(self):
            for line in self._raw.splitlines(keepends=True):
                yield line

    def fake_get_file_handle(_config, _key):
        return S3LikeBody(data)

    with patch("tap_s3_csv.dialect.s3.get_file_handle", fake_get_file_handle):
        dialect.detect_dialect(config, s3_file, table)

    print(
        f"OK  file={csv_path.name} allow_2mb_csv_lines={allow_2mb} "
        f"encoding={table.get('encoding')} delimiter={table.get('delimiter')!r} "
        f"quotechar={table.get('quotechar')!r}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(__file__).parent / "territory_rules_original.csv",
        help="CSV to test (default: territory_rules_original.csv)",
    )
    args = parser.parse_args()

    over_csv = Path(__file__).parent / "territory_rules_over_2mb.csv"
    cases = [
        (args.csv, False, "expect FAIL (default 1 MiB limit)"),
        (args.csv, True, "expect PASS (SP 2 MiB opt-in)"),
        (over_csv, True, "expect FAIL (over 2 MiB even with opt-in)"),
    ]

    exit_code = 0
    for path, allow_2mb, label in cases:
        print(f"\n--- {label} ---")
        try:
            run_detect(path, allow_2mb)
        except Exception as exc:
            print(f"FAIL file={path.name} allow_2mb_csv_lines={allow_2mb}: {exc}")
            if "expect FAIL" in label:
                print("(expected)")
            else:
                exit_code = 1
        else:
            if "expect FAIL" in label:
                print("(unexpected success)")
                exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
