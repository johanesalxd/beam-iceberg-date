package local.beam;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;

/** Emits one string element describing the Java-side Beam schema of the input PCollection<Row>. */
public class DescribeSchemaTransform extends PTransform<PCollection<org.apache.beam.sdk.values.Row>, PCollection<String>> {
  @Override
  public PCollection<String> expand(PCollection<org.apache.beam.sdk.values.Row> input) {
    Schema schema = input.getSchema();
    List<String> lines = new ArrayList<>();
    lines.add("SCHEMA_START");
    for (Schema.Field field : schema.getFields()) {
      lines.add(field.getName() + " | " + field.getType().toString());
    }
    lines.add("SCHEMA_END");
    return input.getPipeline().apply("CreateSchemaDescription", Create.of(lines));
  }
}
