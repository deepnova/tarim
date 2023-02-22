package org.deepexi;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.flink.TableLoader;
import org.deepexi.sink.TarimTableSink;

import java.util.Map;
import java.util.Set;

public class FlinkTarimDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    static final String FACTORY_IDENTIFIER = "tarim";
    public ConnectorTarimTable table;

    public FlinkTarimDynamicTableFactory(FlinkTarimCatalog catalog) {
        this.catalog = catalog;
    }

    public FlinkTarimDynamicTableFactory() {
        this.catalog = null;
    }


    private static CatalogTable loadCatalogTable(Context context) {
        return GET_CATALOG_TABLE.invoke(context);
    }
    private static final DynMethods.UnboundMethod GET_CATALOG_TABLE = DynMethods.builder("getCatalogTable")
            .impl(Context.class, "getCatalogTable")
            .orNoop()
            .build();

    private final FlinkTarimCatalog catalog;
    @Override
    public DynamicTableSink createDynamicTableSink(DynamicTableFactory.Context context) {
        ObjectPath objectPath = context.getObjectIdentifier().toObjectPath();
        CatalogTable catalogTable = loadCatalogTable(context);
        Map<String, String> tableProps = catalogTable.getOptions();
        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());

        TableLoader tableLoader;
        Table table;
        if (catalog != null) {
            table = catalog.getCatalog().loadTable(catalog.toIdentifier(objectPath));
            return new TarimTableSink(table, tableSchema);
        } else {
            //todo
            return null;
        }
        //return new TarimTableSink(table, tableSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier()  {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }
}
