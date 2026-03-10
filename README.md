# Beam Python -> Iceberg DATE Fix via Java Cross-Language Transform

This repository contains a **working solution** for a real Apache Beam Python issue:

> **Beam Python cannot reliably write date-like values into Iceberg `DATE` columns on its own.**

The solution in this repo is:

```text
Python rows
-> Java cross-language cast transform
-> output row with true Beam DATE logical types
-> Iceberg write
```

This allows you to keep your pipeline primarily in **Python**, while using a **tiny Java transform** only for the DATE materialization boundary before the Iceberg sink.

---

## What problem does this solve?

If you try to write date-like Python values directly into Iceberg `DATE` using Beam Python, the default behavior is broken or incompatible in the tested environment.

Earlier investigation showed that the following Python-side inputs do **not** work correctly on their own:

- `datetime.date`
- ISO strings such as `"2026-03-10"`
- epoch-day integers
- custom Python logical type using `beam:logical_type:date:v1`
- Managed IO wrapper

The full investigation history is preserved here:

- **HISTORY.md** — historical debugging trail, failed approaches, and why they were ruled out

This README focuses only on the **current working approach**.

---

## The actual solution

### Core idea

Instead of trying to make Python express Iceberg `DATE` correctly by itself, we insert a small Java transform that explicitly converts selected fields into Beam DATE logical types.

### Flow

```text
Python input rows
  start_date = "2026-03-10"
  end_date   = "2026-03-11"

-> Java cross-language transform

Output row schema
  start_date = LOGICAL_TYPE<beam:logical_type:date:v1>
  end_date   = LOGICAL_TYPE<beam:logical_type:date:v1>

-> Iceberg write

Final Iceberg table
  start_date = DATE
  end_date   = DATE
```

---

## What has been proven in this repo?

### Proven working

The repo now contains **successful end-to-end validation** for the cross-language approach.

#### 1. Java cross-language DATE cast transform runs successfully

We verified at runtime that the transform outputs schema like:

```text
id | INT64 NOT NULL
name | STRING NOT NULL
start_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
end_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
```

#### 2. Fresh Iceberg table creation works

Using Python input rows with string dates, after the Java cast transform, Iceberg is created with:

- `id BIGINT`
- `name VARCHAR`
- `start_date DATE`
- `end_date DATE`

DuckDB verified both:

- schema is correct
- row values are correct

#### 3. Append into existing DATE table works

The same cross-language cast output was successfully appended into an existing Iceberg table whose date columns were already typed as `DATE`.

DuckDB verified the appended rows correctly.

### Bottom line

> **Yes — this repo contains a working fix path for the GitHub issue, without requiring full Java pipeline migration.**

---

## Repository structure

### Primary solution artifacts

#### `xlang-date-cast/`
Java module implementing the cross-language DATE cast transform.

Contains:

- `DateFieldCastTransform.java`
- `DescribeSchemaTransform.java`
- `pom.xml`

This is the most important implementation in the repo.

#### `xlang_date_cast_verify.py`
Python script that verifies the Java transform emits true Beam DATE logical types at runtime.

Use this if you want to confirm the cast transform itself is working before touching Iceberg.

#### `xlang_date_to_iceberg_verify.py`
Python script that verifies the casted output can be written to Iceberg successfully.

It covers:

- fresh table creation
- append into an existing DATE table

This is the main end-to-end proof.

### Supporting / historical artifacts

#### `beam_date_test.py`
Earlier Python-side logical type experiment.
Useful for understanding why the Python-only path looked promising at first, but not sufficient.

#### `beam_iceberg_matrix.py`
Historical Python-only matrix of failed approaches.
Useful background, but **not the final solution**.

#### `HISTORY.md`
Earlier investigation notes and failed branches.
Read this if you want the full debugging / decision trail.

#### `tmp_java_create/`
Small helper used during investigation to create a known-good Iceberg DATE table for append testing.

---

## How to run this repo

## Prerequisites

Validated environment used for the successful runs:

- `uv`
- Apache Beam `2.70.0`
- Java `21.0.2` via mise
- Maven (`mvn`)
- Docker Desktop / Docker engine running
- DuckDB with Iceberg extension available for verification

### Verify Java

```bash
mise exec java@21 -- java -version
```

### Verify Docker

```bash
docker version
```

### Optional: verify DuckDB

```bash
duckdb -c "LOAD iceberg; SELECT version();"
```

---

## Step 1 — Build the Java transform

From repo root:

```bash
cd xlang-date-cast
mise exec java@21 -- mvn -q package
cd ..
```

This builds the jar used by the Python-side cross-language transform.

Expected output artifact:

```text
xlang-date-cast/target/xlang-date-cast-1.0-SNAPSHOT.jar
```

---

## Step 2 — Run the Java unit tests

The Java module now includes small contract tests for the transform itself.

These tests cover the most important accepted and rejected behaviors:

- valid ISO `YYYY-MM-DD` string casting
- valid epoch-day `INT64` casting
- null preservation for nullable date fields
- duplicate configured fields rejected early
- missing configured fields rejected early
- blank string rejected with clear error
- invalid ISO string rejected with clear error

Run from the Java module directory:

```bash
cd xlang-date-cast
mise exec java@21 -- mvn -q test
cd ..
```

Expected result:
- Maven exits successfully
- the contract tests pass

These are **Java-side contract tests**, not end-to-end cross-language integration tests.
They exist so readers can understand exactly what the Java transform accepts and rejects.

---

## Step 3 — Verify the cast transform itself

Run:

```bash
mise exec java@21 -- uv run python xlang_date_cast_verify.py
```

This test checks the cross-language transform boundary.

It proves that:

- Python can invoke the Java transform
- the Java transform executes at runtime
- the transformed output schema contains Beam DATE logical types

Expected output file:

```text
artifacts/xlang_schema_verify/schema-00000-of-00001
```

Expected contents similar to:

```text
SCHEMA_START
id | INT64 NOT NULL
name | STRING NOT NULL
start_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
end_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
SCHEMA_END
```

If you get that, the Java cast transform is working.

---

## Step 4 — Verify fresh table creation with Iceberg

Run:

```bash
mise exec java@21 -- uv run python xlang_date_to_iceberg_verify.py fresh
```

This test does:

```text
Python string dates
-> Java DATE cast transform
-> Iceberg write
```

Expected result:

- success message `FRESH_OK`
- local Iceberg table created under:

```text
artifacts/xlang_to_iceberg/fresh_warehouse/
```

### Verify with DuckDB

```bash
duckdb -c "LOAD iceberg; DESCRIBE SELECT * FROM iceberg_scan('artifacts/xlang_to_iceberg/fresh_warehouse/datecheck/xlang_fresh_dates/metadata/v2.metadata.json');"
```

Expected schema:

- `id BIGINT`
- `name VARCHAR`
- `start_date DATE`
- `end_date DATE`

Read rows:

```bash
duckdb -c "LOAD iceberg; SELECT * FROM iceberg_scan('artifacts/xlang_to_iceberg/fresh_warehouse/datecheck/xlang_fresh_dates/metadata/v2.metadata.json') ORDER BY id;"
```

Expected result includes rows like:

```text
1 | alpha | 2026-03-10 | 2026-03-11
2 | beta  | 1970-01-01 | 1970-01-02
```

---

## Step 4 — Verify append into existing DATE table

This step requires a pre-created local Iceberg table with real DATE columns.

The repo includes a tiny Java helper for that under:

- `tmp_java_create/`

### 4a. Create the canonical DATE table

Run:

```bash
cd tmp_java_create
mise exec java@21 -- mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
CP=$(cat cp.txt):.
mise exec java@21 -- javac -cp "$CP" CreateDateTable.java
rm -rf ../tmp_java_hadoop && mkdir -p ../tmp_java_hadoop/warehouse
mise exec java@21 -- java -cp "$CP" CreateDateTable file:///$(cd ../tmp_java_hadoop/warehouse && pwd) datecheck existing_date_tbl
cd ..
```

Expected schema from the helper:

- `id BIGINT`
- `name VARCHAR`
- `start_date DATE`
- `end_date DATE`

### 4b. Run the append verification

```bash
mise exec java@21 -- uv run python xlang_date_to_iceberg_verify.py append
```

Expected result:

- success message `APPEND_OK`

### Verify with DuckDB

```bash
duckdb -c "LOAD iceberg; SELECT * FROM iceberg_scan('tmp_java_hadoop/warehouse/datecheck/existing_date_tbl/metadata/v2.metadata.json') ORDER BY id;"
```

Expected appended rows:

```text
10 | alpha | 2026-03-10 | 2026-03-11
11 | beta  | 1970-01-01 | 1970-01-02
```

and the schema should remain:

- `id BIGINT`
- `name VARCHAR`
- `start_date DATE`
- `end_date DATE`

---

## What should a customer expect?

If the repo is used correctly, the expected outcome is:

### Input contract

From Python, date-like fields are provided in a stable source representation.

Current proven example uses:

- ISO string columns, e.g. `"2026-03-10"`

### Java transform behavior

The Java transform rewrites selected fields into Beam DATE logical types.

### Output behavior

The downstream Iceberg write sees true DATE logical types and stores them as:

- Iceberg `DATE`

### Practical effect

You can keep your data pipeline in Python and only use Java for the exact place where type correctness matters.

---

## Supported contract / not supported / failure behavior

This section is the operational contract for the Java cross-language transform.

### Supported

#### Field scope
- **Top-level row fields only**
- Explicitly named fields only, for example: `start_date,end_date`
- Unselected fields pass through unchanged

#### Supported input representations for selected DATE fields
- **ISO-8601 date string** in exact date form, e.g. `2026-03-10`
- **Epoch-day integer** as `INT32`
- **Epoch-day integer** as `INT64`
- **Java `LocalDate`** passthrough on the Java side

#### Supported downstream behavior proven in this repo
- Fresh Iceberg table creation with `DATE` columns
- Append into an existing Iceberg table with `DATE` columns
- Python pipeline remains the primary pipeline language; Java is only used at the DATE materialization boundary

### Not supported
- Nested field paths such as `payload.start_date`
- Arrays / maps / nested row traversal
- Custom date formats such as `03/10/2026`
- Timestamp strings such as `2026-03-10T00:00:00Z`
- Blank strings as date values
- Treating this transform as a generic logical-type casting framework

### Fail-fast behavior
The hardened Java transform now fails early for configuration and schema mistakes.

It will error immediately when:
- no field names are configured
- duplicate field names are configured
- a configured field does not exist in the input schema
- a configured field has an unsupported source schema type

It will error at row-processing time when:
- a selected field contains a blank string
- a selected field contains a non-ISO date string
- a selected field contains an unsupported runtime value type

### Recommended customer-facing input contract
For the most predictable production behavior, normalize upstream Python date fields into:

- exact **`YYYY-MM-DD`** strings

before they enter the Java transform.

Epoch-day `INT32` / `INT64` inputs are supported by the Java code, but the most heavily validated path in this repo remains the Python string-date -> Java DATE cast -> Iceberg DATE write flow.

---

## Current limitations / scope

This repo currently proves the path for **selected top-level fields -> DATE**, with the strongest validation on **string `YYYY-MM-DD` input -> DATE**.

Potential future extensions could include:

- epoch-day input as the primary contract
- deeper end-to-end validation of nullable fields
- nested-field support
- support for multiple logical types beyond DATE
  - TIME
  - TIMESTAMP
  - DECIMAL
- publishing the Java transform as a reusable internal library

But those are follow-up improvements. The DATE fix path itself is already proven.

---

## Why this repo exists

This repo exists because the straightforward Python-only approaches were not reliable.

If you want the full debugging history — including what failed and why — read:

- **HISTORY.md**

That file contains the earlier investigation and the reasoning that led us to the cross-language solution.

---

## Recommended next move for real users

If your pipeline is blocked on Beam Python -> Iceberg DATE today, the recommended approach is:

1. keep your upstream pipeline in Python
2. express date columns in a stable source form (currently validated: ISO strings)
3. run them through the Java cross-language cast transform in this repo
4. write the transformed rows to Iceberg

That is currently the smallest working fix with the best tradeoff.

---

## References

- Beam issue #25946: https://github.com/apache/beam/issues/25946
- IcebergIO docs: https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/iceberg/IcebergIO.html
- Beam Python schema internals: `apache_beam/typehints/schemas.py`

---

## Final takeaway

**The Python-only Beam -> Iceberg DATE path is broken in the tested environment, but a tiny Java cross-language cast transform successfully fixes the issue and allows correct Iceberg DATE writes without full Java pipeline migration.**
