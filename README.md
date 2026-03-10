# Beam Python -> Iceberg DATE Investigation

This repository documents a reproducible local investigation into a customer-reported issue:

> **Apache Beam Python cannot write date-like values into an Iceberg `DATE` column correctly.**

This README is now organized in two layers:

1. **Current recommended path** — the most promising solution we have validated so far
2. **Historical investigation trail** — what we tried earlier, what failed, and why

The goal is to help another engineer or customer:

- understand the problem quickly,
- see what has already been tested,
- reproduce the same steps locally,
- understand the most promising direction,
- avoid repeating dead ends.

---

## TL;DR

### Problem

Beam Python does **not** successfully write Iceberg `DATE` from the tested Python-side representations:

- `datetime.date`
- ISO string like `"2024-03-10"`
- epoch-day integer
- custom Python logical type using `beam:logical_type:date:v1`

This was true for:

1. fresh table creation, and
2. append into an existing real Iceberg `DATE` table.

### Most promising solution found so far

The strongest new result in this repo is:

> **A tiny Java cross-language transform can successfully run and emit selected row fields as true Beam portable DATE logical types at runtime.**

That means the best path forward is likely:

```text
Python rows
-> Java cross-language cast transform
-> output row with DATE logical type
-> Iceberg write
```

This is important because it avoids a full Java pipeline migration.

### What is already proven

We have successfully verified Phases 1–4 of that approach:

- Java transform builds successfully
- Python can invoke it through Beam cross-language transform support
- it runs at runtime (not just expansion time)
- its output schema contains:
  - `LOGICAL_TYPE<beam:logical_type:date:v1>`

### What is not yet proven in this README

We have **not yet** completed the next phase in this branch:

- feeding the Java-cast output into Iceberg and verifying end-to-end success

That is the next logical step after this document.

---

## Current Best Path: Tiny Java Cross-Language DATE Cast Transform

## Why this is now the primary direction

The early Python-only attempts showed that Beam Python could not reliably produce Iceberg `DATE`.

But that did **not** mean Java was broken.

In fact, the evidence increasingly pointed to a more specific problem:

> The broken part is the **Python -> Java type boundary**, not necessarily Java Beam or Iceberg itself.

That led to the idea of a **small Java transform** that sits between Python and Iceberg.

Instead of trying to make Python magically produce the exact Java-side date object Iceberg wants, we explicitly insert a Java cast layer:

```text
Python strings or epoch-day values
-> Java transform converts them to Beam DATE fields
-> downstream sink sees real DATE logical types
```

This is much smaller in scope than migrating the whole pipeline to Java.

---

## What was built

A small Java cross-language module was added under:

```text
xlang-date-cast/
```

It contains:

### `local.beam.DateFieldCastTransform`

This transform:

- accepts Beam rows from Python
- rewrites selected fields into Beam `SqlTypes.DATE`
- supports source values for those target date fields as:
  - ISO string like `"2026-03-10"`
  - epoch-day integer / long
  - `LocalDate` passthrough

### `local.beam.DescribeSchemaTransform`

A small helper transform used to inspect the Java-side Beam schema after casting.

This was used to verify that the transformed output really contains DATE logical types.

---

## What was verified

We completed the first 4 phases of this approach.

### Phase 1 — Contract design

Chosen initial contract:

**Input from Python:**
- `id: BIGINT`
- `name: STRING`
- `start_date: STRING`
- `end_date: STRING`

**Java transform config:**
- `date_fields = ["start_date", "end_date"]`

**Expected output:**
- same row shape
- same non-date columns
- selected date fields converted to Beam DATE logical types

### Phase 2 — Java transform build

The Java module was compiled successfully with Java 21.

### Phase 3 — Python invocation

Python successfully invoked the Java transform using Beam cross-language transform support.

### Phase 4 — Runtime schema verification

This is the most important milestone so far.

The Java transform was executed **at runtime**, and the output schema was written to disk.

The runtime output schema was:

```text
SCHEMA_START
id | INT64 NOT NULL
name | STRING NOT NULL
start_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
end_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
SCHEMA_END
```

### What this proves

It proves that:

- the tiny Java transform approach is not just theoretical
- Beam cross-language execution works for this use case
- selected fields can be converted into true Beam portable DATE logical types before the Iceberg sink

This is the strongest positive result currently in the repo.

---

## Why this matters

The earlier negative results told us what does **not** work.

This new positive result tells us what **might** work next.

That changes the engineering recommendation significantly.

### Before this result

The best recommendations were:

1. migrate the sink step to Java, or
2. stage as STRING/BIGINT and convert later

### After this result

We now have a narrower and more attractive option:

> Keep the pipeline mostly in Python, and introduce a tiny Java cross-language DATE cast transform immediately before the Iceberg sink.

This is currently the most promising architecture.

---

## How to reproduce the current best-path verification

## Prerequisites

Working local stack used for the successful runtime verification:

- `uv`
- Apache Beam `2.70.0`
- Java `21.0.2` via mise
- Docker Desktop / Docker engine running

### Verify Java

```bash
mise exec java@21 -- java -version
```

### Verify Docker

```bash
docker version
```

---

## Build the Java transform

From the repo root:

```bash
cd xlang-date-cast
mise exec java@21 -- mvn -q package
```

This produces the jar used by the Python-side verification.

---

## Run the cross-language verification

From the repo root:

```bash
mise exec java@21 -- uv run python xlang_date_cast_verify.py
```

This script:

1. creates sample input rows in Python
2. sends them into the Java date cast transform
3. runs the Java schema description transform
4. writes the result under:

```text
artifacts/xlang_schema_verify/
```

### Expected output

The schema text file should contain something like:

```text
SCHEMA_START
id | INT64 NOT NULL
name | STRING NOT NULL
start_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
end_date | LOGICAL_TYPE<beam:logical_type:date:v1> NOT NULL
SCHEMA_END
```

If you see that, you have reproduced the successful Phase 4 result.

---

## Recommended next step after this README

The next technical step is straightforward:

### Phase 5
Take the Java-cast output and connect it to Iceberg write.

That should be tested in two modes:

1. **Fresh table creation**
2. **Append into existing Iceberg DATE table**

That will determine whether this Java cast layer is only schema-correct, or whether it is the real production fix.

---

## Historical Results: What We Tried Before This

This section is intentionally preserved so that others can understand why the Java cast transform became the primary direction.

## 1. Python-only path: fresh table creation

We tested the following Python-side representations:

- `datetime.date`
- ISO string like `"2024-03-10"`
- epoch-day integer
- custom Python logical type using `beam:logical_type:date:v1` with INT64 base representation
- same custom type with INT32 representation

### Results

| Python input | Resulting Iceberg type |
|---|---|
| `datetime.date` | `timestamptz` |
| ISO `str` | `string` |
| epoch-day `int` | `long` |
| custom logical type (`beam:logical_type:date:v1`, INT64) | `long` |
| custom logical type (`beam:logical_type:date:v1`, INT32) | `int` |

### Conclusion

Beam Python did **not** create an Iceberg `DATE` column in any tested case.

---

## 2. Python-only path: append into existing Iceberg DATE table

A real Iceberg table was pre-created using Java Iceberg with schema:

- `id BIGINT`
- `event_date DATE`

DuckDB verified the resulting schema.

### Results

| Python input | Result |
|---|---|
| ISO `str` | **FAIL** — `String cannot be cast to LocalDate` |
| `datetime.date` | **FAIL** — `Instant cannot be cast to LocalDate` |
| custom logical type (`beam:logical_type:date:v1`) | **FAIL** — `Long cannot be cast to LocalDate` |

### Conclusion

Beam Python did **not** successfully append any tested representation into an existing Iceberg `DATE` column.

---

## 3. Managed IO result

Managed IO was tested to confirm whether the higher-level wrapper changes anything.

### Fresh create via Managed IO

- string input succeeds
- resulting Iceberg type is **`VARCHAR`**, not `DATE`

### Append via Managed IO into existing DATE table

- fails with the same essential error:

```text
String cannot be cast to LocalDate
```

### Conclusion

Managed IO does **not** fix the problem.

It is a different wrapper over the same underlying behavior.

---

## 4. Why the custom Python logical type was not enough

We verified that Python can express a custom logical type with:

- URN: `beam:logical_type:date:v1`
- epoch-day representation
- portable schema metadata

That is what `beam_date_test.py` demonstrates.

However, end-to-end testing showed that this still does **not** become a real Iceberg DATE through the tested Python -> Java Iceberg write path.

So the custom class was a useful experiment, but **not a complete solution**.

---

## Current Recommendation

If you are trying to solve this problem today, the recommendation order is now:

### 1. Best current direction
**Python pipeline + tiny Java cross-language DATE cast transform + Iceberg write**

This is the most promising path validated so far.

### 2. Safe fallback if you need an immediate workaround
**Write `STRING` or `BIGINT` from Python and convert downstream**

Examples:

- write `"2024-03-10"` as `STRING`
- write epoch-day as `BIGINT`
- materialize final Iceberg `DATE` in a downstream step

### 3. Broader alternative
**Use Java Beam for the final Iceberg DATE write step**

This remains a clean option if you want to avoid the Python-side compatibility issue entirely.

---

## Repo Contents

### `beam_date_test.py`
Python-side logical type validation.

### `beam_iceberg_matrix.py`
Earlier end-to-end local matrix for Beam -> Iceberg -> DuckDB.
Primarily useful now as historical evidence of what failed on the Python-only path.

### `xlang-date-cast/`
Java cross-language DATE cast module.
This is now the most important implementation artifact in the repo.

### `xlang_date_cast_verify.py`
Python-side verification script for the Java cross-language DATE cast transform.
This is the key script for reproducing the current best-path result.

### `artifacts/matrix_summary.json`
Machine-readable summary of the earlier Python-only matrix results.

### `artifacts/xlang_schema_verify/`
Output from the Java cross-language schema verification run.

### `tmp_java_create/`
Java helper used to create a canonical local Iceberg DATE table during the earlier investigation.

---

## References

- Beam issue #25946: https://github.com/apache/beam/issues/25946
- IcebergIO docs: https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/iceberg/IcebergIO.html
- Beam managed transforms source: `apache_beam/transforms/managed.py`
- Beam YAML Iceberg wrapper source: `apache_beam/yaml/yaml_io.py`
- Beam Python schema internals: `apache_beam/typehints/schemas.py`

---

## Final Takeaway

**The Python-only Beam -> Iceberg DATE path appears broken in the tested environment. The most promising solution currently validated is a tiny Java cross-language transform that converts selected fields into true Beam DATE logical types before the Iceberg sink.**
