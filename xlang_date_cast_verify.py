from typing import NamedTuple
from pathlib import Path
import shutil

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import JavaExternalTransform
from apache_beam.typehints.schemas import LogicalType, NoArgumentLogicalType
import datetime as dt


@LogicalType.register_logical_type
class PortableDatePreview(NoArgumentLogicalType):
    def __init__(self, argument=""):
        pass

    @classmethod
    def urn(cls):
        return "beam:logical_type:date:v1"

    @classmethod
    def language_type(cls):
        return dt.date

    @classmethod
    def representation_type(cls):
        return int

    @classmethod
    def argument_type(cls):
        return str

    def argument(self):
        return ""

    def to_representation_type(self, value):
        return (value - dt.date(1970, 1, 1)).days

    def to_language_type(self, value):
        return dt.date(1970, 1, 1) + dt.timedelta(days=int(value))

ROOT = Path(__file__).resolve().parent
JAR = ROOT / "xlang-date-cast" / "target" / "xlang-date-cast-1.0-SNAPSHOT.jar"
OUTDIR = ROOT / "artifacts" / "xlang_schema_verify"

class InputRow(NamedTuple):
    id: int
    name: str
    start_date: str
    end_date: str

beam.coders.registry.register_coder(InputRow, beam.coders.RowCoder)


def main():
    if OUTDIR.exists():
        shutil.rmtree(OUTDIR)
    OUTDIR.mkdir(parents=True, exist_ok=True)

    rows = [
        InputRow(1, "alpha", "2026-03-10", "2026-03-11"),
        InputRow(2, "beta", "1970-01-01", "1970-01-02"),
    ]

    opts = PipelineOptions([])

    with beam.Pipeline(options=opts) as p:
        input_rows = p | "CreateInputRows" >> beam.Create(rows).with_output_types(InputRow)

        casted = (
            input_rows
            | "JavaCastDateFields" >> JavaExternalTransform(
                "local.beam.DateFieldCastTransform",
                classpath=[str(JAR)],
            )("start_date,end_date")
        )

        schema_lines = (
            casted
            | "DescribeSchemaInJava" >> JavaExternalTransform(
                "local.beam.DescribeSchemaTransform",
                classpath=[str(JAR)],
            )()
        )

        _ = schema_lines | "WriteSchemaText" >> beam.io.WriteToText(
            str(OUTDIR / "schema"), num_shards=1
        )

    print(f"WROTE_SCHEMA_TO={OUTDIR}")


if __name__ == "__main__":
    main()
