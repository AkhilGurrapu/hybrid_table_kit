# Hybrid Table Materialization — Testing Report

## Summary

**11 models tested against live Snowflake** — all passing on both full-refresh and incremental runs.

- **Environment:** Snowflake account, ADMIN_DB.DBT_HYBRID_TEST schema
- **dbt version:** dbt-core 1.9+ with dbt-snowflake adapter
- **Test phases:** 4 phases covering all features

## Test Results

### Phase 1: Core Features (10 models)

| Model | Features Tested | Full Refresh | Incremental |
|-------|----------------|-------------|-------------|
| basic_hybrid | Simple PK, indexes, MERGE | PASS | PASS |
| composite_pk_hybrid | Composite PK, INDEX INCLUDE | PASS | PASS |
| unique_constraint_hybrid | UNIQUE constraint, named | PASS | PASS |
| default_values_hybrid | DEFAULT values (int, bool, timestamp) | PASS | PASS |
| orders_with_fk_hybrid | Single-column FK, non-CTAS | PASS | PASS |
| multi_col_fk_hybrid | Multi-column FK (composite ref) | PASS | PASS |
| autoincrement_hybrid | AUTOINCREMENT PK, INSERT-only | PASS | PASS |
| delete_insert_hybrid | delete+insert strategy | PASS | PASS |
| schema_change_hybrid | sync_all_columns evolution | PASS | PASS |
| merge_control_hybrid | merge_exclude_columns | PASS | PASS |

### Phase 2: Cross-Schema FK (1 model)

| Model | Features Tested | Full Refresh | Incremental |
|-------|----------------|-------------|-------------|
| cross_schema_fk_hybrid | 3-part dot notation FK | PASS | PASS |

Cross-schema FK tested with both 2-part (`SCHEMA.TABLE`) and 3-part (`DB.SCHEMA.TABLE`) notation. Both validated and working.

### Phase 3: on_schema_change Modes

Tested by mutating the `delete_insert_hybrid` model:

| Mode | Scenario | Result |
|------|----------|--------|
| `sync_all_columns` | Add new column | PASS — ALTER TABLE ADD COLUMN executed |
| `sync_all_columns` | Drop column | PASS — ALTER TABLE DROP COLUMN executed |
| `append_new_columns` | Add new column | PASS — ALTER TABLE ADD COLUMN executed |
| `append_new_columns` | Drop column | PASS — Correctly errored: "does not allow dropping columns" |
| `fail` | Any schema change | PASS — Correctly errored: "on_schema_change=fail" |
| `ignore` | Any schema change | PASS — No ALTER executed, table unchanged |

### Phase 4: Type Change Detection

| Scenario | Result |
|----------|--------|
| Change INT to VARCHAR on incremental | Correctly errored: "requires a full refresh" |
| Re-run with --full-refresh | PASS — Table recreated with new type |

## Bugs Found and Fixed

During development and testing, 15 bugs were discovered and fixed:

| # | Bug | Fix |
|---|-----|-----|
| 1 | `column_definitions.keys()` not sortable | Added `\| sort \| list` |
| 2 | `config.get('incremental_strategy', 'merge')` returns None | Use `(config.get('incremental_strategy') or 'merge')` |
| 3 | Missing commas in CREATE TABLE DDL | Fixed comma placement logic |
| 4 | FK columns listed in UPDATE SET | Added exclusion for PK columns |
| 5 | FK table validation wrong field | Fixed reference identifier resolution |
| 6 | `ALTER HYBRID TABLE` not valid | Changed to `ALTER TABLE` |
| 7 | `fk.name is defined` always true for None values | Changed to `fk.name is not none` |
| 8 | FK via ALTER TABLE fails on hybrid | Forced non-CTAS path when FK present |
| 9 | Missing `-- depends_on:` for FK ordering | Added to models and documented |
| 10 | Type alias mismatch (INT vs NUMBER(38,0)) | Built type canonicalization |
| 11 | Missing `statement('main')` | Added required named statement |
| 12 | AUTOINCREMENT columns in INSERT | Excluded from insert column list |
| 13 | AUTOINCREMENT columns in MERGE | Excluded from merge column lists |
| 14 | `ALTER HYBRID TABLE DROP COLUMN` | Changed to `ALTER TABLE DROP COLUMN` |
| 15 | `adapter.quote()` causes case sensitivity | Removed quoting for ALTER columns |

## Type Canonicalization Coverage

The following type aliases are correctly handled:

| User Type | Canonical (DESCRIBE) | Status |
|-----------|---------------------|--------|
| INT | NUMBER(38,0) | Verified |
| INTEGER | NUMBER(38,0) | Verified |
| BIGINT | NUMBER(38,0) | Verified |
| VARCHAR | VARCHAR(16777216) | Verified |
| VARCHAR(200) | VARCHAR(200) | Verified |
| STRING | VARCHAR(16777216) | Verified |
| DECIMAL(12,2) | NUMBER(12,2) | Verified |
| NUMERIC(10,0) | NUMBER(10,0) | Verified |
| TIMESTAMP_NTZ | TIMESTAMP_NTZ(9) | Verified |
| DATE | DATE | Verified |
| BOOLEAN | BOOLEAN | Verified |
| FLOAT | FLOAT | Verified |

## Constraint Enforcement Verified

| Constraint | Enforcement | Status |
|------------|-------------|--------|
| PRIMARY KEY | Duplicate key INSERT rejected | Verified |
| UNIQUE | Duplicate value INSERT rejected | Verified |
| FOREIGN KEY | Orphan row INSERT rejected | Verified |
| FK cross-schema | Works with 2-part and 3-part notation | Verified |
| NOT NULL | NULL INSERT rejected | Verified |
