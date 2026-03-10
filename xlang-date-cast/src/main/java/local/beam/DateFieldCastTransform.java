package local.beam;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
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
 *
 * This transform intentionally supports only top-level row fields.
 */
public class DateFieldCastTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  private final Set<String> dateFields;

  public DateFieldCastTransform(String commaSeparatedDateFields) {
    List<String> configuredFields = Arrays.stream(commaSeparatedDateFields.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());

    if (configuredFields.isEmpty()) {
      throw new IllegalArgumentException(
          "DateFieldCastTransform requires at least one field name in the comma-separated configuration.");
    }

    Set<String> duplicates = findDuplicates(configuredFields);
    if (!duplicates.isEmpty()) {
      throw new IllegalArgumentException(
          "Duplicate date field(s) configured: " + duplicates);
    }

    this.dateFields = new LinkedHashSet<>(configuredFields);
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    Schema inputSchema = input.getSchema();
    validateConfiguredFields(inputSchema);
    Schema outputSchema = buildOutputSchema(inputSchema);

    return input
        .apply(
            "CastSelectedFieldsToDate",
            MapElements.into(TypeDescriptor.of(Row.class))
                .via(row -> castRow(row, inputSchema, outputSchema)))
        .setRowSchema(outputSchema);
  }

  private void validateConfiguredFields(Schema inputSchema) {
    Set<String> schemaFieldNames = inputSchema.getFields().stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toSet());

    List<String> missingFields = dateFields.stream()
        .filter(fieldName -> !schemaFieldNames.contains(fieldName))
        .collect(Collectors.toList());

    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Configured date field(s) not found in input schema: " + missingFields
              + ". Available fields: " + schemaFieldNames);
    }

    for (Schema.Field field : inputSchema.getFields()) {
      if (!dateFields.contains(field.getName())) {
        continue;
      }
      TypeName typeName = field.getType().getTypeName();
      if (!(typeName == TypeName.STRING
          || typeName == TypeName.INT32
          || typeName == TypeName.INT64
          || typeName == TypeName.LOGICAL_TYPE)) {
        throw new IllegalArgumentException(
            "Field '" + field.getName() + "' has unsupported schema type for DATE casting: "
                + field.getType() + ". Supported source schema types: STRING, INT32, INT64, LOGICAL_TYPE.");
      }
    }
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

  static LocalDate castToLocalDate(Object value, String fieldName) {
    if (value == null) {
      return null;
    }
    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    if (value instanceof String) {
      String text = ((String) value).trim();
      if (text.isEmpty()) {
        throw new IllegalArgumentException(
            "Field '" + fieldName + "' has blank string value. Expected ISO-8601 date string like 2026-03-10.");
      }
      try {
        return LocalDate.parse(text);
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException(
            "Field '" + fieldName + "' has invalid date string value '" + value
                + "'. Expected ISO-8601 date string like 2026-03-10.",
            e);
      }
    }
    if (value instanceof Long) {
      return LocalDate.ofEpochDay((Long) value);
    }
    if (value instanceof Integer) {
      return LocalDate.ofEpochDay(((Integer) value).longValue());
    }
    throw new IllegalArgumentException(
        "Field '" + fieldName + "' has unsupported date source type: "
            + value.getClass().getName()
            + ". Supported runtime types: java.lang.String (ISO-8601 yyyy-MM-dd), java.lang.Integer, java.lang.Long, java.time.LocalDate.");
  }

  private static Set<String> findDuplicates(List<String> values) {
    Set<String> seen = new HashSet<>();
    Set<String> duplicates = new LinkedHashSet<>();
    for (String value : values) {
      if (!seen.add(value)) {
        duplicates.add(value);
      }
    }
    return duplicates;
  }
}
