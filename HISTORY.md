# Beam Python -> Iceberg DATE Investigation History

This file preserves the **original investigation trail** that led to the final solution.

If you want the actual current solution and how to run it, start here instead:

- **README.md** — current working solution, usage, expectations, and repo structure

This historical record is useful if you want to understand:

- the original customer complaint
- what failed on the Python-only path
- why Managed IO was ruled out
- why the Java cross-language transform became the winning approach

---

## Original Summary

The customer-reported issue was:

> **Apache Beam Python cannot write date-like values into an Iceberg `DATE` column correctly.**

We initially explored Python-only options:

- raw `datetime.date`
- ISO string input
- epoch-day integer input
- custom Python logical type using `beam:logical_type:date:v1`
- Managed IO wrapper

The Python-only path consistently failed to create or append true Iceberg `DATE` values.

That investigation ultimately led to the Java cross-language cast solution documented in `README.md`.

---

## Earlier Findings (Python-only path)

### Fresh table creation

| Python input | Resulting Iceberg type |
|---|---|
| `datetime.date` | `timestamptz` |
| ISO `str` | `string` |
| epoch-day `int` | `long` |
| custom logical type (`beam:logical_type:date:v1`, INT64) | `long` |
| custom logical type (`beam:logical_type:date:v1`, INT32) | `int` |

### Append into existing Iceberg DATE table

| Python input | Result |
|---|---|
| ISO `str` | **FAIL** — `String cannot be cast to LocalDate` |
| `datetime.date` | **FAIL** — `Instant cannot be cast to LocalDate` |
| custom logical type (`beam:logical_type:date:v1`) | **FAIL** — `Long cannot be cast to LocalDate` |

### Managed IO

Managed IO was tested and showed the same underlying issue:

- fresh create with string input -> `VARCHAR`
- append into existing DATE table -> same type mismatch failure

Conclusion from the historical investigation:

> The Python-only Beam -> Iceberg DATE path is broken/incompatible in the tested environment.

---

## Why the Java cross-language path won

The turning point was recognizing that:

- Java Beam itself supports DATE semantics correctly
- Iceberg wants Java-side DATE semantics
- the broken boundary is the Python -> Java type bridge

So instead of forcing Python to express Iceberg DATE directly, the solution became:

```text
Python rows
-> Java cross-language cast transform
-> output row with Beam DATE logical types
-> Iceberg write
```

That approach was later validated end-to-end and is now the primary documented solution in `README.md`.

---

## Historical Notes

This history file intentionally stays shorter than the final README.

If you need:
- actual usage instructions
- how to build and run the Java transform
- expected output
- solution architecture
- repo file map

use:

- **README.md**
