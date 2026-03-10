package local.beam;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Cross-language helper transform.
 *
 * Accepts Beam Rows from Python and rewrites selected fields into Beam SqlTypes.DATE.
 * Supported source representations for target date fields:
 * - ISO string: "2026-03-10"
 * - Long / Integer epoch-day values
 * - LocalDate (pass-through)
 */
public class DateFieldCastTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  private final Set<String> dateFields;

  public DateFieldCastTransform(String commaSeparatedDateFields) {
    this.dateFields = Arrays.stream(commaSeparatedDateFields.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toCollection(HashSet::new));
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    Schema inputSchema = input.getSchema();
    Schema outputSchema = buildOutputSchema(inputSchema);

    return input
        .apply(
            "CastSelectedFieldsToDate",
            MapElements.into(TypeDescriptor.of(Row.class))
                .via(row -> castRow(row, inputSchema, outputSchema)))
        .setRowSchema(outputSchema);
  }

  private Schema buildOutputSchema(Schema inputSchema) {
    Schema.Builder builder = Schema.builder();
    for (Schema.Field field : inputSchema.getFields()) {
      if (dateFields.contains(field.getName())) {
        Schema.FieldType dateType = Schema.FieldType.logicalType(SqlTypes.DATE)
            .withNullable(field.getType().getNullable());
        builder.addField(Schema.Field.of(field.getName(), dateType));
      } else {
        builder.addField(field);
      }
    }
    return builder.build();
  }

  private Row castRow(Row row, Schema inputSchema, Schema outputSchema) {
    List<Object> values = new ArrayList<>();
    for (Schema.Field field : inputSchema.getFields()) {
      Object value = row.getValue(field.getName());
      if (!dateFields.contains(field.getName())) {
        values.add(value);
      } else {
        values.add(castToLocalDate(value, field.getName()));
      }
    }
    return Row.withSchema(outputSchema).addValues(values).build();
  }

  private LocalDate castToLocalDate(Object value, String fieldName) {
    if (value == null) {
      return null;
    }
    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    if (value instanceof String) {
      return LocalDate.parse((String) value);
    }
    if (value instanceof Long) {
      return LocalDate.ofEpochDay((Long) value);
    }
    if (value instanceof Integer) {
      return LocalDate.ofEpochDay(((Integer) value).longValue());
    }
    throw new IllegalArgumentException(
        "Field '" + fieldName + "' has unsupported date source type: " + value.getClass().getName());
  }
}
