package org.deepexi.sink;

import com.deepexi.TarimMetaClient;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.deepexi.SerializableTarimTable;
import org.deepexi.TarimPrimaryKey;

public class RowDataTaskWriterFactory implements TaskWriterFactory<RowData> {

    private Table table;
    private RowType flinkSchema;
    private int parallelism;

    private Schema icebergSchema;
    private PartitionSpec partitionSpec;

    private String schemaJson;
    private TarimPrimaryKey primaryKey;
    private int tableId;
    private TarimMetaClient metaClient;

    public RowDataTaskWriterFactory(SerializableTarimTable table, RowType flinkSchema, int parallelism){
        this.table = table;
        this.flinkSchema = flinkSchema;
        this.parallelism = parallelism;
        this.icebergSchema = table.schema();
        this.partitionSpec = table.spec();
        this.schemaJson = table.getSchemaJson();
        this.tableId = table.getTableId();
        this.primaryKey = table.getPrimaryKey();
        this.metaClient = table.getMetaClient();
    }

    @Override
    public void initialize(int taskId, int attemptId) {

    }

    @Override
    public TarimTaskWriter<RowData> create() {

        return new TaskWriter(tableId, primaryKey, flinkSchema, parallelism, icebergSchema, partitionSpec, schemaJson, metaClient);
    }

    public Table getTable() {
        return table;
    }
}
