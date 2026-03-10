package local.beam;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.Test;

class DateFieldCastTransformTest {

  private static final Schema STRING_DATE_SCHEMA = Schema.builder()
      .addInt64Field("id")
      .addStringField("name")
      .addNullableField("start_date", Schema.FieldType.STRING)
      .addNullableField("end_date", Schema.FieldType.STRING)
      .build();

  private static final Schema EPOCH_DAY_SCHEMA = Schema.builder()
      .addInt64Field("id")
      .addNullableField("start_date", Schema.FieldType.INT64)
      .build();

  @Test
  void castsIsoStringDatesAndPreservesUntouchedFields() {
    Pipeline pipeline = Pipeline.create();

    Row input = Row.withSchema(STRING_DATE_SCHEMA)
        .addValues(1L, "alpha", "2026-03-10", "2026-03-11")
        .build();

    PCollection<Row> output = pipeline
        .apply(Create.of(input).withRowSchema(STRING_DATE_SCHEMA))
        .apply(new DateFieldCastTransform("start_date,end_date"));

    assertEquals(SqlTypes.DATE.getIdentifier(), output.getSchema().getField("start_date").getType().getLogicalType().getIdentifier());
    assertEquals(SqlTypes.DATE.getIdentifier(), output.getSchema().getField("end_date").getType().getLogicalType().getIdentifier());
    assertEquals(Schema.TypeName.STRING, output.getSchema().getField("name").getType().getTypeName());

    PAssert.that(output).satisfies(rows -> {
      Row row = rows.iterator().next();
      assertEquals(1L, row.getInt64("id"));
      assertEquals("alpha", row.getString("name"));
      assertEquals(LocalDate.of(2026, 3, 10), row.getLogicalTypeValue("start_date", LocalDate.class));
      assertEquals(LocalDate.of(2026, 3, 11), row.getLogicalTypeValue("end_date", LocalDate.class));
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  void castsEpochDayLongValues() {
    Pipeline pipeline = Pipeline.create();

    Row input = Row.withSchema(EPOCH_DAY_SCHEMA)
        .addValues(1L, 0L)
        .build();

    PCollection<Row> output = pipeline
        .apply(Create.of(input).withRowSchema(EPOCH_DAY_SCHEMA))
        .apply(new DateFieldCastTransform("start_date"));

    PAssert.that(output).satisfies(rows -> {
      Row row = rows.iterator().next();
      assertEquals(LocalDate.of(1970, 1, 1), row.getLogicalTypeValue("start_date", LocalDate.class));
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  void preservesNullValuesForNullableDateFields() {
    Pipeline pipeline = Pipeline.create();

    Row input = Row.withSchema(STRING_DATE_SCHEMA)
        .addValues(1L, "alpha", null, "2026-03-11")
        .build();

    PCollection<Row> output = pipeline
        .apply(Create.of(input).withRowSchema(STRING_DATE_SCHEMA))
        .apply(new DateFieldCastTransform("start_date,end_date"));

    PAssert.that(output).satisfies(rows -> {
      Row row = rows.iterator().next();
      assertNull(row.getValue("start_date"));
      assertEquals(LocalDate.of(2026, 3, 11), row.getLogicalTypeValue("end_date", LocalDate.class));
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  void rejectsDuplicateConfiguredFields() {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> new DateFieldCastTransform("start_date,start_date"));

    assertTrue(error.getMessage().contains("Duplicate date field"));
  }

  @Test
  void rejectsMissingConfiguredFieldAtExpandTime() {
    Pipeline pipeline = Pipeline.create();

    PCollection<Row> input = pipeline.apply(
        Create.of(
            Row.withSchema(STRING_DATE_SCHEMA)
                .addValues(1L, "alpha", "2026-03-10", "2026-03-11")
                .build())
            .withRowSchema(STRING_DATE_SCHEMA));

    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> input.apply(new DateFieldCastTransform("missing_date")));

    assertTrue(error.getMessage().contains("not found in input schema"));
  }

  @Test
  void rejectsBlankDateStringsWithClearMessage() {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> DateFieldCastTransform.castToLocalDate("   ", "start_date"));

    assertTrue(error.getMessage().contains("blank string value"));
  }

  @Test
  void rejectsInvalidIsoDateStringsWithClearMessage() {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> DateFieldCastTransform.castToLocalDate("03/10/2026", "start_date"));

    assertTrue(error.getMessage().contains("Expected ISO-8601 date string"));
  }
}
