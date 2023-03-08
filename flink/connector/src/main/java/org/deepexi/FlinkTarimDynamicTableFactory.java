package org.deepexi;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.TableLoader;
import org.deepexi.sink.TarimTableSink;
import org.deepexi.source.TarimTableSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FlinkTarimDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    static final String FACTORY_IDENTIFIER = "tarim";
    public Table table;

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
        ObjectPath objectPath = context.getObjectIdentifier().toObjectPath();
        //ObjectIdentifier objectIdentifier = context.getObjectIdentifier();
        CatalogTable catalogTable = loadCatalogTable(context);
        Map<String, String> tableProps = catalogTable.getOptions();
        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());

       // TableLoader tableLoader;
        if (catalog != null) {
            //table = catalog.getCatalog().loadTable(catalog.toIdentifier(objectPath));
            //fix the config first
            String CATALOG_NAME = "hadoop_catalog";
            String DATABASE_NAME = "default_db";
            Map<String, String> properties = new HashMap<>();
            properties.put("warehouse", "hdfs://10.201.0.82:9000/wpf0220");
            properties.put("format-version", "2");
            CatalogLoader loader = CatalogLoader.hadoop(CATALOG_NAME, new Configuration(), properties);
            ObjectPath tablePath = new ObjectPath(DATABASE_NAME, "new_table9");

            TableLoader tableLoader = TableLoader.fromCatalog(loader, TableIdentifier.of(Namespace.of(tablePath.getDatabaseName()), tablePath.getObjectName()));
            tableLoader.open();

            return new TarimTableSource(tableLoader, tableSchema, tableProps, context.getConfiguration());

        } else {
            //todo
            return null;
        }


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
