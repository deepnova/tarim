package org.deepexi;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.io.Closeable;
import java.util.*;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

public class FlinkTarimCatalog extends AbstractCatalog {
    private final Catalog tarimCatalog;
    private final Namespace baseNamespace;
    private final SupportsNamespaces asNamespaceCatalog;
    private final Closeable closeable;

    FlinkTarimCatalog(String catalogName, String defaultDatabase, Namespace baseNamespace){
        super(catalogName, defaultDatabase);
        this.baseNamespace = baseNamespace;
        tarimCatalog = new ConnectorTarimCatalog(catalogName);
        this.asNamespaceCatalog = tarimCatalog instanceof SupportsNamespaces ? (SupportsNamespaces)tarimCatalog : null;
        this.closeable = tarimCatalog instanceof Closeable ? (Closeable)tarimCatalog : null;
    }
    private Namespace toNamespace(String database) {
        String[] namespace = new String[this.baseNamespace.levels().length + 1];
        System.arraycopy(this.baseNamespace.levels(), 0, namespace, 0, this.baseNamespace.levels().length);
        namespace[this.baseNamespace.levels().length] = database;
        return Namespace.of(namespace);
    }

    TableIdentifier toIdentifier(ObjectPath path) {
        return TableIdentifier.of(this.toNamespace(path.getDatabaseName()), path.getObjectName());
    }
    Catalog getCatalog(){return tarimCatalog;}

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return null;
    }

    @Override
    public  CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException{
        if (asNamespaceCatalog == null) {
            if (!getDefaultDatabase().equals(databaseName)) {
                throw new DatabaseNotExistException(getName(), databaseName);
            } else {
                return new CatalogDatabaseImpl(Maps.newHashMap(), "");
            }
        } else {
            try {
                Map<String, String> metadata =
                        Maps.newHashMap(asNamespaceCatalog.loadNamespaceMetadata(toNamespace(databaseName)));
                String comment = metadata.remove("comment");
                return new CatalogDatabaseImpl(metadata, comment);
            } catch (NoSuchNamespaceException e) {
                throw new DatabaseNotExistException(getName(), databaseName, e);
            }
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            getDatabase(databaseName);
            return true;
        } catch (DatabaseNotExistException ignore) {
            return false;
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {

    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {

    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }
    private Table loadTarimTable(ObjectPath tablePath) throws TableNotExistException {
        try {
            Table table = tarimCatalog.loadTable(toIdentifier(tablePath));

            return table;
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            throw new TableNotExistException(getName(), tablePath, e);
        }
    }

    static CatalogTable toCatalogTable(Table table) {
        return toCatalogTable(table, table.properties());
    }

    static CatalogTable toCatalogTable(Table table, Map<String, String> properties) {

        //new a fix schema for test
        TypeInformation<?>[] types = new TypeInformation[]{
                STRING_TYPE_INFO,
                INT_TYPE_INFO,
                STRING_TYPE_INFO
        };

        String[] names = new String[]{
           "currency",
           "userid",
           "test1"
        };

        TableSchema schema = new TableSchema(names, types);
        List<String> partitionKeys = new ArrayList<>();
        Map<String, String> tmpProps = new HashMap<>();

        return new CatalogTableImpl(schema, partitionKeys, tmpProps, null);
    }
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        Table table = loadTarimTable(tablePath);
        return toCatalogTable(table);
    }
    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return tarimCatalog.tableExists(this.toIdentifier(tablePath));
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {

    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (Objects.equals(table.getOptions().get("connector"), "tarim")) {
            throw new IllegalArgumentException("Cannot create the table with 'connector'='tarim' table property in an iceberg catalog, Please create table with 'connector'='iceberg' property in a non-iceberg catalog or create table without 'connector'='iceberg' related properties in an iceberg table.");
        } else {
            this.createTarimTable(tablePath, table, ignoreIfExists);
        }
    }

    private static void validateFlinkTable(CatalogBaseTable table) {
        Preconditions.checkArgument(table instanceof CatalogTable, "The Table should be a CatalogTable.");
    }

    private static PartitionSpec toPartitionSpec(List<String> partitionKeys, Schema icebergSchema) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
        Objects.requireNonNull(builder);
        partitionKeys.forEach(builder::identity);
        return builder.build();
    }
    void createTarimTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws CatalogException, TableAlreadyExistException{
        validateFlinkTable(table);

        Schema icebergSchema = FlinkSchemaUtil.convert(table.getSchema());
        PartitionSpec spec = toPartitionSpec(((CatalogTable) table).getPartitionKeys(), icebergSchema);

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        String location = null;
        for (Map.Entry<String, String> entry : table.getOptions().entrySet()) {
            if ("location".equalsIgnoreCase(entry.getKey())) {
                location = entry.getValue();
            } else {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        // update properties from schema
        Map<String, String> propertiesMap = FlinkSchemaUtil.updateTablePropertiesFromSchema(
                ((ResolvedCatalogTable) table).getResolvedSchema());
        properties.putAll(propertiesMap);

        try {
            tarimCatalog.createTable(
                    toIdentifier(tablePath),
                    icebergSchema,
                    spec,
                    location,
                    properties.build());
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath, e);
            }
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {

    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {

    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }


    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTarimDynamicTableFactory(this));
    }
}