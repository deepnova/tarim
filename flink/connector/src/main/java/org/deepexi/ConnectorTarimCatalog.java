package org.deepexi;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Map;

public class ConnectorTarimCatalog implements Catalog {

    private final String catalogName;
    private String databaseName;
    ConnectorTarimCatalog(String catalogName){
        this.catalogName = catalogName;
    }
    public String name() {
        return this.catalogName;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return null;
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
        //todo, to tarimDB
        return new ConnectorTarimTable(identifier.name());
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties) {
        return Catalog.super.createTable(identifier, schema, spec, properties);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
        return Catalog.super.createTable(identifier, schema, spec);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema) {
        return Catalog.super.createTable(identifier, schema);
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
        return Catalog.super.newCreateTableTransaction(identifier, schema, spec, location, properties);
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties) {
        return Catalog.super.newCreateTableTransaction(identifier, schema, spec, properties);
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
        return Catalog.super.newCreateTableTransaction(identifier, schema, spec);
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
        return Catalog.super.newCreateTableTransaction(identifier, schema);
    }

    @Override
    public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties, boolean orCreate) {
        return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, location, properties, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties, boolean orCreate) {
        return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, properties, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, boolean orCreate) {
        return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, boolean orCreate) {
        return Catalog.super.newReplaceTableTransaction(identifier, schema, orCreate);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        return Catalog.super.tableExists(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier) {
        return Catalog.super.dropTable(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
        return false;
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {

    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) {
        //todo, to TarimDB
        return new ConnectorTarimTable(tableIdentifier.name());
    }

    @Override
    public void invalidateTable(TableIdentifier identifier) {
        Catalog.super.invalidateTable(identifier);
    }

    @Override
    public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
        return Catalog.super.registerTable(identifier, metadataFileLocation);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        return Catalog.super.buildTable(identifier, schema);
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        Catalog.super.initialize(name, properties);
    }

}
