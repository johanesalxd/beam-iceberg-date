"""
beam_date_test.py
-----------------
Tests a custom BeamDate logical type that bridges Python's datetime.date
to Beam's portable schema system (URN: beam:logical_type:date:v1).

This is a workaround for Apache Beam issue #25946:
  https://github.com/apache/beam/issues/25946
  "Support more Beam portable schema types as Python types"

The Date, Time, and DateTime logical types are missing from the Python SDK.
BeamDate fills this gap so IcebergIO can map Beam DATE → Iceberg DATE correctly.

Usage:
    uv run python beam_date_test.py
"""

import datetime
import warnings
import sys

warnings.filterwarnings("ignore")

import apache_beam as beam
import apache_beam.typehints.schemas as beam_schemas
from apache_beam.typehints.schemas import LogicalType, NoArgumentLogicalType
from typing import NamedTuple


# ---------------------------------------------------------------------------
# BeamDate logical type
# ---------------------------------------------------------------------------

@LogicalType.register_logical_type
class BeamDate(NoArgumentLogicalType):
    """
    Logical type mapping Python datetime.date to Beam's SqlTypes.DATE.

    Wire format: INT64 (days since Unix epoch 1970-01-01).
    URN must match SqlTypes.DATE.getIdentifier() on the Java side:
        "beam:logical_type:date:v1"

    Note: representation_type returns int (Python) which Beam maps to INT64.
    If the Java IcebergIO connector requires INT32, swap to numpy.int32 here.

    References:
      - Beam portable schema LogicalType:
          sdks/python/apache_beam/typehints/schemas.py
      - IcebergUtils Java type mapping (BEAM_LOGICAL_TYPES_TO_ICEBERG_TYPES):
          sdks/java/io/iceberg/src/main/java/org/apache/beam/sdk/io/iceberg/IcebergUtils.java
      - PR #32688: [Managed Iceberg] Add support for TIMESTAMP, TIME, and DATE types
    """

    URN = "beam:logical_type:date:v1"
    _EPOCH = datetime.date(1970, 1, 1)

    @classmethod
    def urn(cls) -> str:
        return cls.URN

    @classmethod
    def language_type(cls):
        return datetime.date

    @classmethod
    def representation_type(cls):
        return int  # INT64 on wire; see note above re INT32

    def to_representation_type(self, value: datetime.date) -> int:
        """Convert datetime.date → days since epoch."""
        return (value - self._EPOCH).days

    def to_language_type(self, value: int) -> datetime.date:
        """Convert days since epoch → datetime.date."""
        return self._EPOCH + datetime.timedelta(days=value)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

PASS = "✅"
FAIL = "❌"
SEPARATOR = "─" * 55


def section(title: str):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


def check(label: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    print(f"  {status}  {label}" + (f"  [{detail}]" if detail else ""))
    return condition


# ---------------------------------------------------------------------------
# Test 1: Round-trip conversion
# ---------------------------------------------------------------------------

def test_round_trip():
    section("TEST 1 — Round-trip conversion (date ↔ days)")
    bd = BeamDate()
    cases = [
        datetime.date(1970, 1, 1),    # epoch → 0 days
        datetime.date(1969, 12, 31),  # pre-epoch → -1 days
        datetime.date(2000, 1, 1),    # Y2K
        datetime.date(2024, 3, 10),   # random
        datetime.date(2026, 3, 10),   # today
        datetime.date(9999, 12, 31),  # max date
    ]
    all_ok = True
    for d in cases:
        days = bd.to_representation_type(d)
        back = bd.to_language_type(days)
        ok = back == d
        all_ok = all_ok and ok
        check(f"{d}  →  {days:+} days  →  {back}", ok)
    return all_ok


# ---------------------------------------------------------------------------
# Test 2: Registration lookups
# ---------------------------------------------------------------------------

def test_registration():
    section("TEST 2 — LogicalType registry lookups")
    registry = LogicalType._known_logical_types

    found_urn = registry.get_logical_type_by_urn(BeamDate.URN)
    ok1 = check(
        f"Lookup by URN '{BeamDate.URN}'",
        found_urn is BeamDate,
        str(found_urn),
    )

    found_type = registry.get_logical_type_by_language_type(datetime.date)
    ok2 = check(
        "Lookup by language type datetime.date",
        found_type is BeamDate,
        str(found_type),
    )

    return ok1 and ok2


# ---------------------------------------------------------------------------
# Module-level schema definition (must be at module scope for Beam pickling)
# ---------------------------------------------------------------------------

class EventRecord(NamedTuple):
    id: int
    name: str
    event_date: datetime.date  # routes through BeamDate logical type


# ---------------------------------------------------------------------------
# Test 3: NamedTuple schema generation
# ---------------------------------------------------------------------------

def test_schema():
    section("TEST 3 — NamedTuple schema field type")

    schema = beam_schemas.named_tuple_to_schema(EventRecord)  # module-level class
    date_field = next((f for f in schema.fields if f.name == "event_date"), None)

    ok1 = check("event_date field exists in schema", date_field is not None)
    if not date_field:
        return False

    has_logical = date_field.type.HasField("logical_type")
    ok2 = check("field type is logical_type (not atomic)", has_logical)

    if has_logical:
        urn_ok = date_field.type.logical_type.urn == BeamDate.URN
        ok3 = check(
            f"URN = '{date_field.type.logical_type.urn}'",
            urn_ok,
        )
        print(f"\n  Raw proto for event_date:\n{date_field.type}")
    else:
        ok3 = False

    return ok1 and ok2 and ok3


# ---------------------------------------------------------------------------
# Test 4: DirectRunner pipeline
# ---------------------------------------------------------------------------

def _format_record(record: EventRecord) -> str:
    """Format a record as a string for text sink output."""
    return f"{record.id}|{record.name}|{record.event_date.isoformat()}"


def test_pipeline():
    section("TEST 4 — DirectRunner pipeline")
    import tempfile, os, glob

    # EventRecord defined at module scope (required for Beam pickling)
    records = [
        EventRecord(1, "Alpha", datetime.date(2024, 3, 10)),
        EventRecord(2, "Beta",  datetime.date(2026, 1, 1)),
        EventRecord(3, "Gamma", datetime.date(1999, 12, 31)),
        EventRecord(4, "Delta", datetime.date(1970, 1, 1)),   # epoch
        EventRecord(5, "Eps",   datetime.date(1969, 12, 31)), # pre-epoch
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        out_prefix = os.path.join(tmpdir, "out")

        with beam.Pipeline() as p:
            (
                p
                | "Create"    >> beam.Create(records)
                | "Format"    >> beam.Map(_format_record)
                | "WriteSink" >> beam.io.WriteToText(out_prefix)
            )

        ok1 = check("Pipeline completed without error", True)

        # Read back output shards
        output_files = glob.glob(out_prefix + "*")
        lines = []
        for f in sorted(output_files):
            with open(f) as fh:
                lines.extend(line.strip() for line in fh if line.strip())

        ok2 = check(f"All {len(records)} records written", len(lines) == len(records))

        print(f"\n  Output records:")
        for line in sorted(lines):
            print(f"    {line}")

        # Verify all expected date strings are present
        expected_dates = {r.event_date.isoformat() for r in records}
        found_dates = {line.split("|")[2] for line in lines}
        ok3 = check("All date values present in output", expected_dates == found_dates)

    return ok1 and ok2 and ok3


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'=' * 55}")
    print("  BeamDate Logical Type — Test Suite")
    print(f"  Apache Beam {beam.__version__}")
    print(f"  Issue: github.com/apache/beam/issues/25946")
    print(f"{'=' * 55}")

    results = {
        "Round-trip conversion": test_round_trip(),
        "Registry lookups":      test_registration(),
        "Schema generation":     test_schema(),
        "DirectRunner pipeline": test_pipeline(),
    }

    section("SUMMARY")
    all_pass = True
    for name, ok in results.items():
        check(name, ok)
        all_pass = all_pass and ok

    print(f"\n  {'ALL TESTS PASSED ✅' if all_pass else 'SOME TESTS FAILED ❌'}")
    print()

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
