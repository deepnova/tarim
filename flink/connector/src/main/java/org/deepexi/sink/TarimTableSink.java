package org.deepexi.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class TarimTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
    private boolean overwrite = false;
    private final TableSchema tableSchema;
    private Table table;

    private TarimTableSink(TarimTableSink toCopy) {
        this.table = toCopy.table;
        this.tableSchema = toCopy.tableSchema;
        this.overwrite = toCopy.overwrite;
    }

    public TarimTableSink(Table table, TableSchema tableSchema) {
        this.table = table;
        this.tableSchema = tableSchema;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Preconditions.checkState(!overwrite || context.isBounded(),
                "Unbounded data stream doesn't support overwrite operation.");

        List<String> equalityColumns = tableSchema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .orElseGet(ImmutableList::of);

        return (DataStreamSinkProvider) dataStream -> TarimSink.forRowData(dataStream)
                .table(table)
                .tableSchema(tableSchema)
                .overwrite(overwrite)
                .append();
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // The flink's PartitionFanoutWriter will handle the static partition write policy automatically.
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            builder.addContainedKind(kind);
        }
        return builder.build();
    }

    @Override
    public DynamicTableSink copy() {
        return new TarimTableSink(this);
    }

    @Override
    public String asSummaryString() {
        return "Tarim table sink";
    }

    @Override
    public void applyOverwrite(boolean newOverwrite) {
        this.overwrite = newOverwrite;
    }
}
