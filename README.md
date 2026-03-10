# Beam Python -> Iceberg DATE Investigation

This repository documents a reproducible local investigation into a customer-reported issue:

> **Apache Beam Python cannot write date-like values into an Iceberg `DATE` column correctly.**

The goal of this document is to help another engineer or customer:

- understand the problem quickly,
- see exactly what was tested,
- reproduce the same tests locally,
- understand what works vs what fails,
- choose a practical workaround.

---

## TL;DR

### Customer complaint

> A Beam Python pipeline cannot write a Python date value into an Iceberg `DATE` column.

### Final conclusion

**The complaint is legitimate in the tested local environment.**

We tested both:

1. **Fresh table creation** through Beam Python -> Iceberg
2. **Append into an already-existing Iceberg `DATE` table**

Across all tested Python-side representations, Beam Python did **not** successfully write an Iceberg `DATE` value.

### What failed

#### Fresh table creation

| Python input | What Beam/Iceberg created instead |
|---|---|
| `datetime.date` | `timestamptz` |
| ISO string like `"2024-03-10"` | `string` |
| epoch-day integer | `long` |
| custom logical type `beam:logical_type:date:v1` (INT64) | `long` |
| custom logical type `beam:logical_type:date:v1` (INT32) | `int` |

#### Append into an existing Iceberg `DATE` table

| Python input | Result |
|---|---|
| ISO string | **FAIL** — `String cannot be cast to LocalDate` |
| `datetime.date` | **FAIL** — `Instant cannot be cast to LocalDate` |
| custom logical type `beam:logical_type:date:v1` | **FAIL** — `Long cannot be cast to LocalDate` |

### Managed IO result

We also tested **Managed IO** to verify whether the higher-level wrapper changes the outcome.

**It does not.**

- Managed IO fresh create with string input creates **`VARCHAR`**, not `DATE`
- Managed IO append into an existing `DATE` table fails with the same core type mismatch

### Practical takeaway

If you need a reliable production workaround today, the safest options are:

1. **Use Java Beam for the Iceberg DATE write step**, or
2. **Write `STRING` or `BIGINT` from Python and convert downstream**

---

## Problem Background

The most likely upstream clue is Apache Beam issue **#25946**:

- https://github.com/apache/beam/issues/25946

That issue notes that the Python SDK is missing portable schema logical types such as:

- `Date`
- `Time`
- `DateTime`

At first glance, this makes the bug report plausible:

- Beam Java understands portable logical types for date/time values
- Iceberg expects Beam `SqlTypes.DATE -> Iceberg DATE`
- Python Beam may not be able to express that type correctly

That led to the initial hypothesis:

> Maybe the problem can be fixed by creating a custom Python logical type for `datetime.date`.

We tested that hypothesis end-to-end. It was **not enough**.

---

## Scope of What Was Tested

We tested the following Python-side representations:

1. `datetime.date`
2. ISO string, e.g. `"2024-03-10"`
3. epoch-day integer, e.g. `20522`
4. custom portable logical type using URN `beam:logical_type:date:v1` with:
   - INT64 representation
   - INT32 representation

We tested them in two different scenarios:

### Scenario A — Fresh table creation

Beam creates the Iceberg table based on the Python-side schema.

### Scenario B — Append into an existing real Iceberg `DATE` table

The Iceberg table was pre-created outside Beam using the Java Iceberg API with schema:

- `id BIGINT`
- `event_date DATE`

This second scenario matters because it answers the practical question:

> Even if Beam cannot create the correct schema, can it still write into a real DATE table that already exists?

In our tests, the answer was **no**.

---

## Important Design Choices in This Investigation

### Why DuckDB was used for verification, not as the primary table authority

DuckDB was used to:

- inspect Iceberg metadata,
- verify final column types,
- read the resulting tables back.

That was useful because it gives us a second system to confirm what Beam actually wrote.

However, for the authoritative append test, we did **not** rely on DuckDB to create the canonical DATE table.

Reason:

- Beam writes through the Java Iceberg + Hadoop catalog path
- if DuckDB created the table and Beam failed against it, there would be ambiguity about whether the failure came from:
  - Beam Python,
  - Java Iceberg,
  - or a catalog compatibility difference

So the real DATE table was created using Java Iceberg itself.

That gave us a much stronger control case.

### Why Managed IO was tested separately

Beam Python has a higher-level Managed IO wrapper for Iceberg.

We tested it to answer a simple question:

> Does the wrapper fix the problem?

It does **not**.

Managed IO still hits the same underlying Java Iceberg write path, so the behavior is materially the same.

---

## Exact Findings

## 1. Fresh Table Creation Results

This section answers:

> If Beam Python creates the table, what Iceberg type does it actually create?

| Python input | Resulting Iceberg type | Outcome |
|---|---|---|
| `datetime.date` | `timestamptz` | write succeeds, wrong type |
| ISO `str` | `string` | write succeeds, wrong type |
| epoch-day `int` | `long` | write succeeds, wrong type |
| custom logical type `beam:logical_type:date:v1` with INT64 | `long` | write succeeds, wrong type |
| custom logical type `beam:logical_type:date:v1` with INT32 | `int` | write succeeds, wrong type |

### Conclusion from fresh-create tests

**Beam Python did not create an Iceberg `DATE` column in any tested case.**

Even the custom logical type path did not materialize as Iceberg `DATE`.

---

## 2. Append Into Existing Iceberg DATE Table

This section answers:

> If the table already exists with a real `DATE` column, can Beam Python write into it?

The table was pre-created with Java Iceberg and verified via DuckDB as:

- `id BIGINT`
- `event_date DATE`

### Append results

| Python input | Result |
|---|---|
| ISO `str` | **FAIL** — `java.lang.String cannot be cast to java.time.LocalDate` |
| `datetime.date` | **FAIL** — `org.joda.time.Instant cannot be cast to java.time.LocalDate` |
| custom logical type `beam:logical_type:date:v1` | **FAIL** — `java.lang.Long cannot be cast to java.time.LocalDate` |

### Conclusion from append tests

**Beam Python did not successfully append any tested representation into an existing Iceberg `DATE` column.**

This is the strongest evidence in the repo, because the table schema is known-good and independently verified.

---

## 3. Managed IO Result

Managed IO was tested as a separate check.

### Fresh create via Managed IO

- string input successfully writes
- resulting Iceberg type is **`VARCHAR`**, not `DATE`

### Append via Managed IO into existing `DATE` table

- fails with the same essential error as the lower-level path:

```text
String cannot be cast to LocalDate
```

### Conclusion

**Managed IO does not fix the issue.**

It is a different wrapper around the same underlying behavior.

---

## Why the Custom Logical Type Did Not Solve It

One of the main goals of this repo was to test whether a custom Python logical type could bridge the gap.

We verified that Python can successfully express a custom logical type with:

- URN: `beam:logical_type:date:v1`
- epoch-day representation
- portable schema metadata

That part is covered by `beam_date_test.py`.

However, end-to-end results show that this still does **not** become a true Iceberg `DATE` in the tested Beam Python -> Java Iceberg write path.

So the custom class is useful as a Python-side experiment, but **it is not a working production fix here**.

---

## Reproducing the Investigation Locally

## Prerequisites

Working local stack used for all meaningful validation:

- `uv`
- Apache Beam `2.70.0`
- DuckDB `v1.4.4` with Iceberg extension
- **Java 21.0.2 via mise**

### Verify Java

```bash
mise exec java@21 -- java -version
```

---

## Step 1 — Python-side logical type test

Run:

```bash
uv run python beam_date_test.py
```

What this proves:

- the custom logical type is syntactically valid in Python
- the logical type can be registered
- the schema metadata can be generated
- a DirectRunner pipeline can carry the values

What this does **not** prove:

- successful Iceberg DATE writes

---

## Step 2 — End-to-end create-path matrix

Run:

```bash
mise exec java@21 -- uv run python beam_iceberg_matrix.py
```

This script:

- writes multiple Python-side variants through Beam Python -> Iceberg
- reads the results back with DuckDB
- records summary output in:

```text
artifacts/matrix_summary.json
```

---

## Step 3 — Existing DATE-table append test

The repo also includes a tiny Java helper used during the investigation:

- `tmp_java_create/CreateDateTable.java`
- `tmp_java_create/pom.xml`

This helper creates a real local Iceberg table with:

- `id BIGINT`
- `event_date DATE`

That table was then used as the target for Beam Python append tests.

This is the key test if you want to answer:

> Can Beam Python write into a real DATE table that already exists?

In our local runs, the answer was **no**.

---

## What We Recommend to Customers

Given the tested behavior, these are the practical options.

### Option 1 — Use Java Beam for the final Iceberg DATE write

This is the cleanest technical fix if native Iceberg `DATE` is required.

Why:

- Beam Java has native logical type support for DATE
- avoids the Python-side logical type compatibility gap

### Option 2 — Write `STRING` or `BIGINT` from Python, convert downstream

Examples:

- write `"2024-03-10"` as `STRING`
- or write epoch-day as `BIGINT`
- cast to `DATE` in a later step

This is the simplest operational workaround if the pipeline must stay in Python.

### Option 3 — Split the pipeline into staging + materialization

Pattern:

```text
Python Beam -> staging data (STRING or BIGINT date field)
           -> downstream transform / materialization job
           -> final Iceberg DATE table
```

This is often the most practical compromise.

### Option 4 — Raise / track an upstream Beam issue with this evidence

This repo now contains enough evidence for a useful upstream bug report or follow-up against Beam issue #25946.

---

## Files in This Repo

### `beam_date_test.py`
Python-side logical-type validation.

### `beam_iceberg_matrix.py`
End-to-end local matrix for Beam -> Iceberg -> DuckDB.

### `artifacts/matrix_summary.json`
Machine-readable summary of the most recent matrix run.

### `tmp_java_create/`
Java helper used to create a canonical local Iceberg `DATE` table.

---

## References

- Beam issue #25946: https://github.com/apache/beam/issues/25946
- IcebergIO docs: https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/iceberg/IcebergIO.html
- Beam managed transforms source: `apache_beam/transforms/managed.py`
- Beam YAML Iceberg wrapper source: `apache_beam/yaml/yaml_io.py`
- Beam Python schema internals: `apache_beam/typehints/schemas.py`

---

## Final Takeaway

**In the tested local environment, Beam Python date-like values do not successfully map to Iceberg `DATE`, either when creating the table or when appending into an existing `DATE` column.**
