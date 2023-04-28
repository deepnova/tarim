package org.deepexi.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.flink.IcebergTableSource;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.deepexi.ConnectorTarimTable;
import org.deepexi.FlinkSqlPrimaryKey;

import java.util.*;

public class TarimTableSource implements ScanTableSource, SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown {
    private final int[] projectedFields;
    private final long limit;
    private List<Expression> filters;

    private final TableLoader tableLoader;
    private final TableSchema schema;
    private final Map<String, String> properties;
    private final boolean isLimitPushDown;
    private final ReadableConfig readableConfig;

    private Table tarimTable;

    private boolean otherFilter = false;
    private boolean partitionEqFilter = false;
    private boolean partitionKeyFilter = false;
    private boolean primaryKeyFilter = false;

    private Set<Object> partitionKeys;
    private Set<FlinkSqlPrimaryKey> primaryKeys;

    private TarimTableSource(TarimTableSource toCopy) {
        this.tarimTable = toCopy.tarimTable;
        this.tableLoader = toCopy.tableLoader;
        this.schema = toCopy.schema;
        this.properties = toCopy.properties;
        this.projectedFields = toCopy.projectedFields;
        this.isLimitPushDown = toCopy.isLimitPushDown;
        this.limit = toCopy.limit;
        this.filters = toCopy.filters;
        this.readableConfig = toCopy.readableConfig;
    }

    public TarimTableSource(Table tarimTable, TableLoader tableLoader, TableSchema schema, Map<String, String> properties,
                            ReadableConfig readableConfig) {
        this(tarimTable, tableLoader, schema, properties, null, false, -1, ImmutableList.of(), readableConfig);
    }

    private TarimTableSource(Table tarimTable, TableLoader tableLoader, TableSchema schema, Map<String, String> properties,
                               int[] projectedFields, boolean isLimitPushDown,
                               long limit, List<Expression> filters, ReadableConfig readableConfig) {
        this.tarimTable = tarimTable;
        this.tableLoader = tableLoader;
        this.schema = schema;
        this.properties = properties;
        this.projectedFields = projectedFields;
        this.isLimitPushDown = isLimitPushDown;
        this.limit = limit;
        this.filters = filters;
        this.readableConfig = readableConfig;
    }

    public void setPrimaryKeyFilter(boolean primaryKeyFilter) {
        this.primaryKeyFilter = primaryKeyFilter;
    }

    public void setPartitionKeyFilter(boolean partitionKeyFilter){
        this.partitionKeyFilter = partitionKeyFilter;
    }

    public void setOtherFilter(boolean otherFilter){
        this.otherFilter = otherFilter;
    }

    public void setPartitionEqFilter(boolean partitionEqFilter){
        this.partitionEqFilter = partitionEqFilter;
    }

    public void setPartitionKeys(Set<Object> partitionKeys){
        this.partitionKeys = partitionKeys;
    }

    public void setPrimaryKeys(Set<FlinkSqlPrimaryKey> primaryKeys){
        this.primaryKeys = primaryKeys;
    }

    private DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
        return TarimSource.forRowData()
                .env(execEnv)
                .tableLoader(tableLoader)
                .properties(properties)
                .table(tarimTable)
                .otherFilter(otherFilter)
                .primaryKeyFilter(primaryKeyFilter)
                .partitionKeyFilter(partitionKeyFilter)
                .partitionEqFilter(partitionEqFilter)
                .partitionKey(partitionKeys)
                .primaryKey(primaryKeys)
                //.project(getProjectedSchema())
               //.limit(limit)
                .filters(filters)
               // .flinkConf(readableConfig)
                .build();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider(ScanTableSource.ScanContext runtimeProviderContext) {
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                return createDataStream(execEnv);
            }

            @Override
            public boolean isBounded() {
                return TarimSource.isBounded(properties);
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new TarimTableSource(this);
    }

    @Override
    public String asSummaryString() {
        return null;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> flinkFilters) {
        List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
        List<Expression> expressions = Lists.newArrayList();

        List<String> partitionKeys = ((ConnectorTarimTable)tarimTable).getPartitionKey();
        //todo the primaryKey maybe many
        List<String> primaryKeys;
        primaryKeys = ((ConnectorTarimTable)tarimTable).getPrimaryKey().getPrimaryKeys();

        for (ResolvedExpression resolvedExpression : flinkFilters) {
            TarimFlinkFilters.convert(resolvedExpression,partitionKeys, primaryKeys);
            if (TarimFlinkFilters.otherFilter){
                setOtherFilter(true);
            }
            if (TarimFlinkFilters.primaryKeyFilter){
                setPrimaryKeyFilter(true);
            }
            if (TarimFlinkFilters.partitionFilter){
                setPartitionKeyFilter(true);
            }
            if (TarimFlinkFilters.partitionEqFilter){
                setPartitionEqFilter(true);
            }

            //TarimFlinkFilters.otherFilter = false;
            //TarimFlinkFilters.primaryKeyFilter = false;
            //TarimFlinkFilters.partitionFilter = false;

            Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
            if (icebergExpression.isPresent()) {
                expressions.add(icebergExpression.get());
                acceptedFilters.add(resolvedExpression);
            }
        }

        setPartitionKeys(TarimFlinkFilters.partitionValues);
        setPrimaryKeys(TarimFlinkFilters.flinkSqlPrimaryKeys);

        this.filters = expressions;
        return Result.of(acceptedFilters, flinkFilters);
    }

    @Override
    public void applyLimit(long l) {

    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] ints) {

    }

}
