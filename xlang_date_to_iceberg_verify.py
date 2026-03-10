from typing import NamedTuple
from pathlib import Path
import shutil
import yaml
import datetime as dt

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import JavaExternalTransform, MANAGED_SCHEMA_TRANSFORM_IDENTIFIER, SchemaAwareExternalTransform
from apache_beam.portability.common_urns import ManagedTransforms
from apache_beam.transforms.managed import _resolve_expansion_service
from apache_beam.typehints.schemas import LogicalType, NoArgumentLogicalType


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
ART = ROOT / "artifacts" / "xlang_to_iceberg"
FRESH_WH = ART / "fresh_warehouse"
EXISTING_WH = ROOT / "tmp_java_hadoop" / "warehouse"

class InputRow(NamedTuple):
    id: int
    name: str
    start_date: str
    end_date: str

beam.coders.registry.register_coder(InputRow, beam.coders.RowCoder)


def java_cast(pcoll):
    return pcoll | "JavaCastDateFields" >> JavaExternalTransform(
        "local.beam.DateFieldCastTransform",
        classpath=[str(JAR)],
    )("start_date,end_date")


def fresh_create():
    if FRESH_WH.parent.exists():
        shutil.rmtree(FRESH_WH.parent)
    FRESH_WH.mkdir(parents=True, exist_ok=True)

    rows = [
        InputRow(1, "alpha", "2026-03-10", "2026-03-11"),
        InputRow(2, "beta", "1970-01-01", "1970-01-02"),
    ]

    config = {
        'table': 'datecheck.xlang_fresh_dates',
        'catalog_name': 'default',
        'catalog_properties': {
            'type': 'hadoop',
            'warehouse': str(FRESH_WH.resolve()),
        }
    }
    opts = PipelineOptions(['--experiments=enable_managed_transforms'])
    expansion = _resolve_expansion_service('iceberg', ManagedTransforms.Urns.ICEBERG_WRITE.urn, None, opts)

    with beam.Pipeline(options=opts) as p:
        _ = (
            p
            | "CreateFreshRows" >> beam.Create(rows).with_output_types(InputRow)
            | "CastFreshDates" >> beam.Map(lambda x: x)
            | "JavaCastFreshDates" >> JavaExternalTransform(
                "local.beam.DateFieldCastTransform",
                classpath=[str(JAR)],
            )("start_date,end_date")
            | "WriteFreshIceberg" >> SchemaAwareExternalTransform(
                identifier=MANAGED_SCHEMA_TRANSFORM_IDENTIFIER,
                expansion_service=expansion,
                rearrange_based_on_discovery=True,
                transform_identifier=ManagedTransforms.Urns.ICEBERG_WRITE.urn,
                config=yaml.dump(config),
            )
        )


def append_existing():
    rows = [
        InputRow(10, "alpha", "2026-03-10", "2026-03-11"),
        InputRow(11, "beta", "1970-01-01", "1970-01-02"),
    ]

    config = {
        'table': 'datecheck.existing_date_tbl',
        'catalog_name': 'default',
        'catalog_properties': {
            'type': 'hadoop',
            'warehouse': str(EXISTING_WH.resolve()),
        }
    }
    opts = PipelineOptions(['--experiments=enable_managed_transforms'])
    expansion = _resolve_expansion_service('iceberg', ManagedTransforms.Urns.ICEBERG_WRITE.urn, None, opts)

    with beam.Pipeline(options=opts) as p:
        _ = (
            p
            | "CreateExistingRows" >> beam.Create(rows).with_output_types(InputRow)
            | "CastExistingDates" >> beam.Map(lambda x: x)
            | "JavaCastExistingDates" >> JavaExternalTransform(
                "local.beam.DateFieldCastTransform",
                classpath=[str(JAR)],
            )("start_date,end_date")
            | "WriteExistingIceberg" >> SchemaAwareExternalTransform(
                identifier=MANAGED_SCHEMA_TRANSFORM_IDENTIFIER,
                expansion_service=expansion,
                rearrange_based_on_discovery=True,
                transform_identifier=ManagedTransforms.Urns.ICEBERG_WRITE.urn,
                config=yaml.dump(config),
            )
        )


if __name__ == "__main__":
    import sys
    mode = sys.argv[1]
    if mode == 'fresh':
        fresh_create()
        print('FRESH_OK')
    elif mode == 'append':
        append_existing()
        print('APPEND_OK')
    else:
        raise SystemExit('usage: fresh|append')
