import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

public class CreateDateTable {
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException("Usage: CreateDateTable <warehouseUri> <namespace> <table>");
    }
    String warehouse = args[0];
    String namespace = args[1];
    String tableName = args[2];

    Schema schema = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "start_date", Types.DateType.get()),
      Types.NestedField.required(4, "end_date", Types.DateType.get())
    );

    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehouse);
    Namespace ns = Namespace.of(namespace);
    try {
      catalog.createNamespace(ns);
    } catch (Exception ignored) {}
    TableIdentifier ident = TableIdentifier.of(ns, tableName);
    try {
      catalog.dropTable(ident, true);
    } catch (Exception ignored) {}

    Table table = catalog.createTable(ident, schema);
    System.out.println("CREATED=" + table.location());
    System.out.println("SCHEMA=" + table.schema());
  }
}
