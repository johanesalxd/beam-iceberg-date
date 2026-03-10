"""
End-to-end Beam -> Iceberg -> DuckDB validation harness for Python date support.

Run with Java 21 via mise:
    mise exec java@21 -- uv run python beam_iceberg_matrix.py

Why Java 21?
- Java 26 failed locally with Hadoop/Iceberg:
  UnsupportedOperationException: getSubject is not supported

This harness avoids beam.managed.Write(...) directly and calls the underlying
Iceberg schema transform via SchemaAwareExternalTransform.
"""

import argparse
import datetime as dt
import glob
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid
from typing import Any, NamedTuple

import yaml

PASS = "✅"
FAIL = "❌"

ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "artifacts"
WAREHOUSE = ARTIFACTS / "warehouse"
SUMMARY_JSON = ARTIFACTS / "matrix_summary.json"


class RecordDate(NamedTuple):
    id: int
    event_date: dt.date


class RecordString(NamedTuple):
    id: int
    event_date: str


class RecordInt(NamedTuple):
    id: int
    event_date: int


def print_header(title: str):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=check)


def duckdb_query_json(sql: str) -> list[dict[str, Any]]:
    cmd = ["duckdb", "-json", "-c", f"LOAD iceberg; {sql}"]
    cp = run(cmd, check=True)
    out = cp.stdout.strip()
    return json.loads(out) if out else []


def latest_metadata_json(table_dir: Path) -> Path:
    candidates = sorted(table_dir.glob("metadata/v*.metadata.json"))
    if not candidates:
        raise FileNotFoundError(f"No metadata json found under {table_dir}")
    return candidates[-1]


def infer_table_dir(table: str) -> Path:
    namespace, name = table.split(".", 1)
    return WAREHOUSE / namespace / name


def inspect_iceberg(table: str) -> dict[str, Any]:
    table_dir = infer_table_dir(table)
    metadata_json = latest_metadata_json(table_dir)

    describe_rows = duckdb_query_json(
        f"DESCRIBE SELECT * FROM iceberg_scan('{metadata_json}')"
    )
    data_rows = duckdb_query_json(
        f"SELECT * FROM iceberg_scan('{metadata_json}') ORDER BY id"
    )

    # Use Python JSON for schema metadata; DuckDB flattens nested arrays/structs
    # into stringified representations for this file.
    metadata_doc = json.loads(Path(metadata_json).read_text())
    schema_fields = []
    schemas = metadata_doc.get("schemas", [])
    if schemas:
        schema_fields = schemas[0].get("fields", [])

    return {
        "table_dir": str(table_dir),
        "metadata_json": str(metadata_json),
        "describe": describe_rows,
        "rows": data_rows,
        "schema_fields": schema_fields,
    }


def write_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Worker mode
# ---------------------------------------------------------------------------

def worker_register_variant(variant: str):
    import apache_beam.typehints.schemas as beam_schemas
    from apache_beam.typehints.schemas import LogicalType, NoArgumentLogicalType

    if variant == "custom_int64":
        @LogicalType.register_logical_type
        class BeamDateInt64(NoArgumentLogicalType):
            URN = "beam:logical_type:date:v1"
            EPOCH = dt.date(1970, 1, 1)

            @classmethod
            def urn(cls):
                return cls.URN

            @classmethod
            def language_type(cls):
                return dt.date

            @classmethod
            def representation_type(cls):
                return int

            def to_representation_type(self, value: dt.date) -> int:
                return (value - self.EPOCH).days

            def to_language_type(self, value: int) -> dt.date:
                return self.EPOCH + dt.timedelta(days=value)

        return beam_schemas

    if variant == "custom_int32":
        import numpy as np

        @LogicalType.register_logical_type
        class BeamDateInt32(NoArgumentLogicalType):
            URN = "beam:logical_type:date:v1"
            EPOCH = dt.date(1970, 1, 1)

            @classmethod
            def urn(cls):
                return cls.URN

            @classmethod
            def language_type(cls):
                return dt.date

            @classmethod
            def representation_type(cls):
                return np.int32

            def to_representation_type(self, value: dt.date):
                return np.int32((value - self.EPOCH).days)

            def to_language_type(self, value) -> dt.date:
                return self.EPOCH + dt.timedelta(days=int(value))

        return beam_schemas

    return None


def worker_rows_for_variant(variant: str):
    d1 = dt.date(2024, 3, 10)
    d2 = dt.date(1970, 1, 1)
    days1 = (d1 - dt.date(1970, 1, 1)).days
    days2 = 0

    if variant in ("custom_int64", "custom_int32", "raw_date"):
        return RecordDate, [
            RecordDate(1, d1),
            RecordDate(2, d2),
        ]
    elif variant == "raw_string":
        return RecordString, [
            RecordString(1, "2024-03-10"),
            RecordString(2, "1970-01-01"),
        ]
    elif variant == "raw_int_days":
        return RecordInt, [
            RecordInt(1, days1),
            RecordInt(2, days2),
        ]
    else:
        raise ValueError(f"Unknown variant: {variant}")


def worker_run(mode: str, variant: str, table: str, out_json: Path):
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.portability.common_urns import ManagedTransforms
    from apache_beam.transforms.external import (
        MANAGED_SCHEMA_TRANSFORM_IDENTIFIER,
        SchemaAwareExternalTransform,
    )
    from apache_beam.transforms.managed import _resolve_expansion_service

    result: dict[str, Any] = {
        "mode": mode,
        "variant": variant,
        "table": table,
        "ok": False,
    }

    try:
        worker_register_variant(variant)

        opts = PipelineOptions(["--experiments=enable_managed_transforms"])
        expansion = _resolve_expansion_service(
            "iceberg", ManagedTransforms.Urns.ICEBERG_WRITE.urn, None, opts
        )

        config = {
            "table": table,
            "catalog_name": "default",
            "catalog_properties": {
                "type": "hadoop",
                "warehouse": str(WAREHOUSE.resolve()),
            },
        }

        record_type, rows = worker_rows_for_variant(variant)
        beam.coders.registry.register_coder(record_type, beam.coders.RowCoder)

        with beam.Pipeline(options=opts) as p:
            _ = (
                p
                | "CreateRows" >> beam.Create(rows).with_output_types(record_type)
                | "IcebergWrite" >> SchemaAwareExternalTransform(
                    identifier=MANAGED_SCHEMA_TRANSFORM_IDENTIFIER,
                    expansion_service=expansion,
                    rearrange_based_on_discovery=True,
                    transform_identifier=ManagedTransforms.Urns.ICEBERG_WRITE.urn,
                    config=yaml.dump(config),
                )
            )

        result["inspection"] = inspect_iceberg(table)
        result["ok"] = True
    except Exception as e:
        result["ok"] = False
        result["error_type"] = type(e).__name__
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()

    write_json(out_json, result)


# ---------------------------------------------------------------------------
# Orchestrator mode
# ---------------------------------------------------------------------------

def invoke_worker(mode: str, variant: str, table: str) -> dict[str, Any]:
    out_json = ARTIFACTS / "runs" / f"{mode}_{variant}.json"
    if out_json.exists():
        out_json.unlink()

    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--mode",
        mode,
        "--variant",
        variant,
        "--table",
        table,
        "--out-json",
        str(out_json),
    ]
    cp = run(cmd, cwd=ROOT, check=False)

    result = json.loads(out_json.read_text()) if out_json.exists() else {
        "mode": mode,
        "variant": variant,
        "table": table,
        "ok": False,
        "error_type": "WorkerFailure",
        "error": f"worker exited {cp.returncode}",
        "stdout": cp.stdout,
        "stderr": cp.stderr,
    }
    result["worker_stdout"] = cp.stdout
    result["worker_stderr"] = cp.stderr
    result["worker_exit_code"] = cp.returncode
    return result


def summarize_schema_field(inspection: dict[str, Any]) -> str | None:
    for field in inspection.get("schema_fields", []):
        if field.get("name") == "event_date":
            return field.get("type")
    return None


def main_orchestrator():
    print_header("Beam -> Iceberg -> DuckDB date validation matrix")
    print(f"Warehouse: {WAREHOUSE}")

    if ARTIFACTS.exists():
        shutil.rmtree(ARTIFACTS)
    WAREHOUSE.mkdir(parents=True, exist_ok=True)

    namespace = "datecheck"

    create_variants = [
        "custom_int64",
        "custom_int32",
        "raw_date",
        "raw_string",
        "raw_int_days",
    ]

    create_results = []
    date_table_candidates = []

    print_header("Phase 1 — fresh table creation")
    for variant in create_variants:
        table = f"{namespace}.create_{variant}_{uuid.uuid4().hex[:8]}"
        print(f"Running create variant: {variant} -> {table}")
        result = invoke_worker("create", variant, table)
        create_results.append(result)
        if result.get("ok"):
            field_type = summarize_schema_field(result["inspection"])
            print(f"  {PASS} write ok | event_date Iceberg type = {field_type}")
            if field_type == "date":
                date_table_candidates.append(result)
        else:
            print(f"  {FAIL} {result.get('error_type')}: {result.get('error')}"[:500])

    canonical = next((r for r in date_table_candidates), None)

    append_results = []
    print_header("Phase 2 — append into existing DATE table")
    if canonical:
        canonical_table = canonical["table"]
        print(f"Canonical DATE table chosen: {canonical_table}")
        append_variants = [
            "raw_date",
            "raw_string",
            "raw_int_days",
            "custom_int64",
            "custom_int32",
        ]
        for variant in append_variants:
            print(f"Running append variant: {variant} -> {canonical_table}")
            result = invoke_worker("append", variant, canonical_table)
            append_results.append(result)
            if result.get("ok"):
                field_type = summarize_schema_field(result["inspection"])
                row_count = len(result["inspection"].get("rows", []))
                print(f"  {PASS} append ok | event_date type = {field_type} | rows now = {row_count}")
            else:
                print(f"  {FAIL} {result.get('error_type')}: {result.get('error')}"[:500])
    else:
        print(f"{FAIL} No DATE-creating variant found; append phase skipped")

    summary = {
        "create_results": create_results,
        "append_results": append_results,
        "canonical_date_table": canonical["table"] if canonical else None,
    }
    write_json(SUMMARY_JSON, summary)

    print_header("Verdict snapshot")
    print("CREATE:")
    for r in create_results:
        if r.get("ok"):
            field_type = summarize_schema_field(r["inspection"])
            print(f"  {PASS} {r['variant']:<12} -> {field_type}")
        else:
            print(f"  {FAIL} {r['variant']:<12} -> {r.get('error_type')}")

    print("\nAPPEND:")
    if append_results:
        for r in append_results:
            if r.get("ok"):
                field_type = summarize_schema_field(r["inspection"])
                print(f"  {PASS} {r['variant']:<12} -> {field_type}")
            else:
                print(f"  {FAIL} {r['variant']:<12} -> {r.get('error_type')}")
    else:
        print("  (skipped)")

    print(f"\nFull summary written to: {SUMMARY_JSON}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--worker", action="store_true")
    p.add_argument("--mode")
    p.add_argument("--variant")
    p.add_argument("--table")
    p.add_argument("--out-json")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.worker:
        worker_run(args.mode, args.variant, args.table, Path(args.out_json))
    else:
        main_orchestrator()
