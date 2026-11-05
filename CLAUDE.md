# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

`supportfunctions` is a shared internal Python package (published/installed as a dependency into other
projects, e.g. `mainplace-datacraft`) that provides thin wrappers around SQL Server and Postgres
connections for ETL-style workloads: opening connections with retry, bulk upsert/merge of pandas
DataFrames, and declarative database/table creation from YAML config. It also has a small Azure Key
Vault / local-file secrets helper.

All source lives in a single module: [src/supportfunctions/support_functions.py](src/supportfunctions/support_functions.py).
Public API is re-exported from [src/supportfunctions/__init__.py](src/supportfunctions/__init__.py):
`sqlConnection` (SQL Server / pyodbc), `postgresConnection` (Postgres / psycopg2), `secrets`.

## Versioning — required on every change

**Every change to `src/supportfunctions/` must bump the `version` field in [pyproject.toml](pyproject.toml)**
(currently semver, e.g. `0.1.33` -> `0.1.34`). This package is consumed by other repos as a pinned
dependency, so an unbumped version means downstream projects silently don't get the fix. Bump the patch
version for fixes/small changes; use judgement (minor/major) for breaking API changes. Do this as part of
the same change, not as an afterthought.

## Commands

There is no build step, linter, or test suite configured in this repo (no `tests/`, no lint config). To
sanity-check a change without a full install:

```bash
python -m py_compile src/supportfunctions/support_functions.py
python -c "import sys; sys.path.insert(0, 'src'); import supportfunctions"
```

Install locally into a consuming project in editable mode for integration testing:

```bash
pip install -e /path/to/utilities-supportfunctions
```

## Architecture

- **`sqlConnection`** (SQL Server, via `pyodbc`) and **`postgresConnection`** (Postgres, via `psycopg2`)
  are structurally parallel classes with almost identical method sets, but they are *not* shared via a
  base class — each has its own `writeMany`, `writeManyEntity`, `parseValues`, `constructDatabase`, etc.
  When fixing a bug in one class's method, check whether the same bug exists in the other class's
  equivalent method (this has historically been a source of drift/duplicated bugs, e.g. `parseValues`).
- **`writeMany` / `writeManyEntity`**: bulk-write a DataFrame to a table, either as a merge/upsert
  (`merge=True`, requires `mergeKeys`) or a full truncate-and-insert (`merge=False`). `writeManyEntity`
  additionally scopes the write to one entity (deletes/matches only rows for that entity) and logs the
  entity in the sync log — used for per-account/per-customer incremental syncs. SQL Server uses a
  row-by-row `MERGE` statement; Postgres uses `INSERT ... ON CONFLICT DO UPDATE` with `execute_values`,
  and de-duplicates rows by `mergeKeys` first since Postgres errors if the same target row is touched
  twice in one `ON CONFLICT` statement.
- **`_ensure_connection`** runs before each `execSingle`/`writeMany`/`writeManyEntity`. In
  `postgresConnection` it rolls back an aborted transaction and reconnects a closed connection, but only
  sends `SELECT 1` after `_PING_AFTER_SECONDS` of inactivity, so a connection dropped within that window
  raises `OperationalError` once and recovers on the next call. Writes are deliberately not retried.
  `sqlConnection` still pings before every call.
- **`parseValues`**: converts a DataFrame into a list of row-tuples suitable for the DB driver, handling
  NaN/None -> `NULL` conversion and per-column dtype coercion (notably booleans, which need their own
  branch so they aren't written as the string `"True"/"False"`). Uses the module-level `_isNa()` helper
  instead of calling `pd.isna()` directly on a cell value, because `pd.isna()` raises `ValueError` when
  given an array-like value (list/tuple/set/dict/np.ndarray) — such values are treated as "not NA".
- **`constructDatabase`**: creates schema/tables/constraints/indexes/foreign keys from a YAML config
  (see [examples/sample_configDB.yaml](examples/sample_configDB.yaml) for SQL Server and
  [examples/sample_configDB_postgres.yaml](examples/sample_configDB_postgres.yaml) for Postgres — the two
  are not interchangeable, since column DDL is written per-dialect in the YAML itself). Table creation
  order is monitor -> meta -> fact; foreign keys are applied only after all tables in the batch exist, so
  FK target tables don't need to be declared before the tables that reference them.
  `postgresConnection._quoteColumnDefs` additionally quotes each column identifier in the raw DDL string
  so camelCase names survive Postgres's default lowercasing of unquoted identifiers.
- **`secrets`**: reads/writes secrets either from Azure Key Vault (`local=False`) or a local
  `credentials.yaml` file (`local=True`), with an optional name `prefix`. Used so the same calling code
  works against a local dev credentials file and a deployed Key Vault.
- Identifiers (table/schema/column names) are always quoted via `_quoteIdent` before being interpolated
  into SQL strings (`[...]` for SQL Server, `"..."` for Postgres) — column/table names come from config,
  not user input, but this also protects against reserved-word collisions and casing issues.
