# Sales Planning CSV line-limit local test

Customer file: `Rule_Based_Assignment___Territory_Rules-*.csv` (Territory Rules stream).

| File | Max physical line | Expected with `allow_2mb_csv_lines` |
| --- | --- | --- |
| `territory_rules_original.csv` | ~1.13 MiB | **Pass** (between 1 MiB default and 2 MiB SP cap) |
| `territory_rules_over_2mb.csv` | ~2.26 MiB | **Fail** (`Too many bytes in one line`) |

Production sets `allow_2mb_csv_lines: true` only for Tqp imports (`singerUtils.ts`).

## Quick repro (no S3)

From repo root:

```bash
cd /Users/dgomez/Documents/GitHub/tap-s3-csv
poetry install
poetry run python local-test/sp-line-limit/repro_line_limit.py
```

## Full `tap-s3-csv` path (substitute staged SP file)

Skip `tap-tqp` when you already have the CSV. Mimic post–`tap-tqp` config:

1. Upload the CSV under your import prefix, e.g.  
   `s3://YOUR_BUCKET/your-prefix/Rule Based Assignment | Territory Rules-7728-<uuid>.csv`  
   (filename must match the `search_pattern` for that stream in the config `tap-tqp` writes.)

2. Minimal config (merge with your real bucket/prefix/table entry):

```json
{
  "bucket": "YOUR_BUCKET",
  "prefix": "your-prefix",
  "allow_2mb_csv_lines": true,
  "delimiter": ",",
  "quotechar": "\"",
  "tables": [
    {
      "table_name": "Rule Based Assignment | Territory Rules",
      "search_prefix": "your-prefix/",
      "search_pattern": "Rule Based Assignment \\| Territory Rules\\x2d7728\\x2d.*\\.csv.*",
      "delimiter": ",",
      "quotechar": "\"",
      "recursive_search": false
    }
  ]
}
```

3. Discover (runs dialect detection — where the line limit is enforced):

```bash
poetry run tap-s3-csv --config /path/to/config.json --discover > catalog.json
```

4. Swap `territory_rules_over_2mb.csv` into S3 (same key) and re-run discover; expect failure even with `allow_2mb_csv_lines: true`.

## End-to-end on personal stage

Use `import-activity` with a branch that pins `tap-s3-csv` `WP-35779-20260929-182333` (see `docker_build.sh`), trigger a Tqp import, and replace the staged object in the temp upload prefix before `tap-s3-csv` runs — or run steps 1–3 against the same bucket/prefix the import uses.
