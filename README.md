# beam-iceberg-date

Workaround + validation harness for Apache Beam Python date support with Iceberg.

## Background

Customer symptom:

> Python Beam pipeline cannot write `date`-typed data to an Iceberg `DATE` column.

Likely upstream root cause:

- Apache Beam issue **#25946** — Python SDK is missing portable schema logical types such as **Date**, **Time**, and **DateTime**.
- Issue link: https://github.com/apache/beam/issues/25946

Beam's Iceberg connector expects Beam logical types for date/time columns:

- `SqlTypes.DATE -> Iceberg DATE`
- `SqlTypes.TIME -> Iceberg TIME`
- `SqlTypes.DATETIME -> Iceberg TIMESTAMP`

But Python Beam does not natively expose a `Date` logical type.

## What this repo contains

### 1. `beam_date_test.py`
Python-side proof that a custom logical type can bridge:

```text
Python datetime.date
  -> Beam logical type urn=beam:logical_type:date:v1
  -> portable schema protobuf
```

It verifies:

- round-trip conversion (`date <-> days since epoch`)
- logical type registration
- NamedTuple schema generation
- DirectRunner pipeline survival

Run:

```bash
uv run python beam_date_test.py
```

### 2. `beam_iceberg_matrix.py`
End-to-end local validation harness for:

- writing rows through Beam Python into local Iceberg tables
- reading them back via DuckDB Iceberg extension
- testing multiple input shapes / variants

Variants covered:

- raw `datetime.date`
- raw ISO `str`
- raw integer days since epoch
- custom `BeamDate` with **INT64** representation
- custom `BeamDate` with **INT32** representation

It tests both:

- **fresh table creation**
- **append into an existing Iceberg DATE table**

Run with Java 21 via mise:

```bash
mise exec java@21 -- uv run python beam_iceberg_matrix.py
```

## Important runtime note

### Java 26 is too new for this path

We confirmed local Iceberg writes fail under Java 26 with Hadoop security internals:

```text
UnsupportedOperationException: getSubject is not supported
```

So the validated local runtime for Beam + Hadoop-backed Iceberg here is:

- **Java 21 via mise**

## Implementation notes

### No `beam.managed.Write(...)` in the harness

The end-to-end harness invokes the underlying Iceberg schema transform directly through:

- `SchemaAwareExternalTransform`
- `MANAGED_SCHEMA_TRANSFORM_IDENTIFIER`
- underlying URN = `beam:schematransform:org.apache.beam:iceberg_write:v1`

Reason: Python Beam does not expose a first-class `apache_beam.io.iceberg` module, so direct Iceberg access from Python flows through Beam's Java-backed schema transform surface.

## Current hypotheses to validate

1. A custom Python logical type with URN `beam:logical_type:date:v1` may be enough for Iceberg DATE writes.
2. The remaining unknown is whether the Java side accepts:
   - INT64 representation, or
   - requires INT32 representation.
3. String writes may work differently depending on whether:
   - Beam is creating the table, or
   - writing to a pre-existing Iceberg `DATE` column.

## Results (local)

### Fresh table creation via Beam Python -> Iceberg -> DuckDB

| Variant | Resulting Iceberg type | Outcome |
|---|---|---|
| raw `datetime.date` | `timestamptz` | write succeeds, wrong type |
| raw ISO `str` | `string` | write succeeds, wrong type |
| raw int days since epoch | `long` | write succeeds, wrong type |
| custom `BeamDate` INT64 (`beam:logical_type:date:v1`) | `long` | write succeeds, wrong type |
| custom `BeamDate` INT32 | `int` | write succeeds, wrong type |

**Conclusion:** none of the tested Python-side inputs produced an Iceberg `DATE` column on fresh table creation.

### Append into pre-created Iceberg `DATE` table

A real Iceberg table was pre-created locally using Java Iceberg + Hadoop catalog with schema:

- `id BIGINT`
- `event_date DATE`

Append results from Beam Python:

| Variant | Outcome |
|---|---|
| raw ISO `str` | **FAIL** — `java.lang.String cannot be cast to java.time.LocalDate` |
| raw `datetime.date` | **FAIL** — `org.joda.time.Instant cannot be cast to java.time.LocalDate` |
| custom `BeamDate` INT64 (`beam:logical_type:date:v1`) | **FAIL** — `java.lang.Long cannot be cast to java.time.LocalDate` |

**Conclusion:** Beam Python did not successfully append any tested Python representation into an existing Iceberg `DATE` column in this local environment.

## Practical recommendation

Given the local evidence, the safest options are:

1. **Use Java Beam for the Iceberg DATE write path**
2. **Write as `STRING` or `BIGINT` from Python**, then convert downstream
3. **Raise / track upstream Beam issue** with reproducible evidence

## Expected outcomes

The original target matrix has now been resolved for the locally tested cases above.

## Local stack

- `uv`
- Apache Beam `2.70.0`
- DuckDB `v1.4.4` with Iceberg extension installed
- Java `21.0.2` via mise

## References

- Beam issue #25946: https://github.com/apache/beam/issues/25946
- IcebergIO docs: https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/iceberg/IcebergIO.html
- Beam managed transforms source: `apache_beam/transforms/managed.py`
- Beam YAML Iceberg wrapper source: `apache_beam/yaml/yaml_io.py`
