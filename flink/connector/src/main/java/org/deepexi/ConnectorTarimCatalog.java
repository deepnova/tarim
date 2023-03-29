package org.deepexi;

import com.deepexi.TarimMetaClient;
import com.deepexi.rpc.TarimProto;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import org.apache.iceberg.*;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

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

        //todo :tmp ip and port
        final String host = "127.0.0.1";
        final int port = 1301;

        String tableName = tableIdentifier.name();
        String DbName;
        Namespace nameSpace = tableIdentifier.namespace();
        try{
            DbName = nameSpace.level(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TarimMetaClient metaClient = new TarimMetaClient(host, port);

        TarimProto.GetTableResponse  response = metaClient.loadTableRequest(catalogName, DbName, tableName);
        try{
            if (response.getCode() != 0){
                return null;

            }else{
                String tarimTableSchema = response.getTable();

                org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema = new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(tarimTableSchema);

                TableSchema.Builder builder = new TableSchema.Builder();

                for ( org.apache.iceberg.shaded.org.apache.avro.Schema.Field fieid : avroSchema.getFields()){
                    builder.field(fieid.name(), TarimTypeToFlinkType.convertToDataType(fieid.schema().toString()).notNull());
                }

                builder.primaryKey(response.getPrimaryKey());

                List<String> partitionKey = new ArrayList<>();
                if (response.getParitionKeysCount() > 0) {
                    //todo, support one key for test now
                    partitionKey.add(response.getParitionKeys(0));
                }

                TableSchema tableSchema = builder.build();
                Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

                //can not use jasonFormat, because the key is different between iceberg and grpc-protobuf-message
                //TarimProto.PartitionSpecOrBuilder specMessage = response.getPartitionSpecOrBuilder();
                //String json = JsonFormat.printer().print(specMessage);

                JSONObject fieldObj = new JSONObject();

                JSONArray array = new JSONArray();
                JSONObject fieldSpec = new JSONObject();

                int i = 0;
                for (TarimProto.Fields fields: response.getPartitionSpec().getFieldsList()){

                    fieldObj.put("name", fields.getName());
                    fieldObj.put("transform", fields.getTransform());
                    fieldObj.put("source-id", fields.getSourceID());
                    fieldObj.put("field-id", fields.getFiledID());

                    array.put(i, fieldObj);
                    i++;
                }

                fieldSpec.put("spec-id", response.getPartitionSpec().getSpecID());
                fieldSpec.put("fields", array);
                String jsonString = fieldSpec.toString();
                PartitionSpec  partitionSpec = PartitionSpecParser.fromJson(icebergSchema, jsonString);

                return new ConnectorTarimTable(tableIdentifier.name(), response.getTableID(), tableSchema, partitionKey, partitionSpec, icebergSchema, tarimTableSchema, response.getPrimaryKey());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


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
