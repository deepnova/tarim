package org.deepexi.source;

import com.deepexi.KvNode;
import com.deepexi.TarimMetaClient;
import com.deepexi.rpc.TarimProto;
import com.google.protobuf.ByteString;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.calcite.shaded.org.apache.commons.codec.digest.MurmurHash3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.expressions.In;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.*;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import org.apache.iceberg.relocated.com.google.common.base.Predicate;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.deepexi.ConnectorTarimTable;
import org.deepexi.FlinkSqlPrimaryKey;
import org.deepexi.TarimDbAdapt;
import org.deepexi.TarimPrimaryKey;

import static org.deepexi.TarimUtil.serialize;

public class TarimSource {
    private TarimSource() {
    }

    public static Builder forRowData() {
        return new Builder();
    }

    public static class Builder {
        private StreamExecutionEnvironment env;
        private Table icebergTable;
        private Table tarimTable;
        private TableLoader tableLoader;
        private TableSchema projectedSchema;
        private ReadableConfig readableConfig = new Configuration();
        public TarimDbAdapt tarimDbAdapt = new TarimDbAdapt(0, 0, null);
        private final TarimScanContext.Builder  contextBuilder = TarimScanContext.builder();

        private TarimMetaClient metaClient;

        private boolean primaryKeyFilter;
        private boolean partitionKeyFilter;
        private boolean otherFilter;

        public Builder table(Table newTable) {
            this.tarimTable = newTable;
            return this;
        }

        public Builder tableLoader(TableLoader newLoader) {
            this.tableLoader = newLoader;
            return this;
        }

        public Builder env(StreamExecutionEnvironment newEnv) {
            this.env = newEnv;
            return this;
        }


        public Builder filters(List<Expression> filters) {
            contextBuilder.filters(filters);
            return this;
        }

        public Builder project(TableSchema schema) {
            this.projectedSchema = schema;
            return this;
        }

        public Builder limit(long newLimit) {
            contextBuilder.limit(newLimit);
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            contextBuilder.fromProperties(properties);
            return this;
        }

        public Builder caseSensitive(boolean caseSensitive) {
            contextBuilder.caseSensitive(caseSensitive);
            return this;
        }

        public Builder snapshotId(Long snapshotId) {
            contextBuilder.useSnapshotId(snapshotId);
            return this;
        }

        public Builder startSnapshotId(Long startSnapshotId) {
            contextBuilder.startSnapshotId(startSnapshotId);
            return this;
        }

        public Builder endSnapshotId(Long endSnapshotId) {
            contextBuilder.endSnapshotId(endSnapshotId);
            return this;
        }

        public Builder asOfTimestamp(Long asOfTimestamp) {
            contextBuilder.asOfTimestamp(asOfTimestamp);
            return this;
        }

        public Builder splitSize(Long splitSize) {
            contextBuilder.splitSize(splitSize);
            return this;
        }

        public Builder splitLookback(Integer splitLookback) {
            contextBuilder.splitLookback(splitLookback);
            return this;
        }

        public Builder splitOpenFileCost(Long splitOpenFileCost) {
            contextBuilder.splitOpenFileCost(splitOpenFileCost);
            return this;
        }

        public Builder streaming(boolean streaming) {
            contextBuilder.streaming(streaming);
            return this;
        }

        public Builder nameMapping(String nameMapping) {
            contextBuilder.nameMapping(nameMapping);
            return this;
        }

        public Builder flinkConf(ReadableConfig config) {
            this.readableConfig = config;
            return this;
        }

        public Builder primaryKeyFilter(boolean primaryKeyFilter) {
            contextBuilder.primaryKeyFilter(primaryKeyFilter);
            return this;
        }

        public Builder partitionKeyFilter(boolean partitionKeyFilter) {
            contextBuilder.partitionKeyFilter(partitionKeyFilter);
            return this;
        }

        public Builder partitionEqFilter(boolean partitionKeyFilter) {
            contextBuilder.partitionEqFilter(partitionKeyFilter);
            return this;
        }

        public Builder otherFilter(boolean otherFilter) {
            contextBuilder.otherFilter(otherFilter);
            return this;
        }

        public Builder partitionKey(Set<String> partitionKeys) {
            contextBuilder.partitionKey(partitionKeys);
            return this;
        }

        public Builder primaryKey(Set<FlinkSqlPrimaryKey> primaryKeys) {
            contextBuilder.primaryKey(primaryKeys);
            return this;
        }

        public DataStream<RowData> build() {
            Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
            boolean allPartitionFlag = false;
            boolean doLookup = false;

            Iterator<String> iter = contextBuilder.partitionKeys.iterator();
            String primaryStr = "";
            String partitionStr = "";


            TarimPrimaryKey tarimPrimaryKey = ((ConnectorTarimTable)tarimTable).getPrimaryKey();
            List<String> primaryKey = tarimPrimaryKey.getPrimaryKeys();

            List<String> primaryKeyValues = new ArrayList<>();

            if (contextBuilder.getOtherFilter()){
                allPartitionFlag = true;
            }else if (contextBuilder.getPartitionKeyFilter()){
                if (contextBuilder.getPartitionEqFilter() && contextBuilder.getPrimaryKeyFilter()){
                    //support 1 partitionKey now
                    if (contextBuilder.partitionKeys.size() == 1){
                        partitionStr = iter.next();

                        for (String pk: primaryKey){
                            FluentIterable<FlinkSqlPrimaryKey> filter =
                                    FluentIterable.from(contextBuilder.primaryKeys).filter(new Predicate<FlinkSqlPrimaryKey>(){
                                        @Override
                                        public boolean apply(FlinkSqlPrimaryKey primaryKey) {
                                            return primaryKey.getPrimaryKey().equals(pk);
                                        }
                                    });

                            int i = 0;
                            for (FlinkSqlPrimaryKey key: filter){
                                primaryKeyValues.add(key.getValue());
                                i++;
                            }
                            if (i != 1){
                                break;
                            }
                        }
                        doLookup = true;
                    }
                }
            }

            if (doLookup){
                //todo
                Schema schema = tarimTable.schema();
                int tableID = ((ConnectorTarimTable)tarimTable).getTableId();
                TypeInformation<RowData> typeInfo = FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(schema));
                metaClient = new TarimMetaClient("127.0.0.1", 1301);
                String partitionID = getPartitionID((ConnectorTarimTable)tarimTable, partitionStr);

                long chunkID = partitionID.hashCode() & 0x00000000FFFFFFFFL;
                long hash = MurmurHash3.hash32(chunkID) & 0x00000000FFFFFFFFL;
                KvNode node = metaClient.getMasterReplicaNode(hash);
                TarimProto.DbStatusResponse response = metaClient.partitionRequest(tableID, partitionID);
                //todo check
                if (node.host == null || response.getCode() != 0){
                    throw new RuntimeException("the result is incorrect from meta node!!");
                }

                LookupSourceFunction testFun = new LookupSourceFunction(schema, tableID, ((ConnectorTarimTable)tarimTable).getSchemaJson(),
                        partitionID, tarimPrimaryKey.codecPrimaryValue(primaryKeyValues),
                        typeInfo, node.host, node.port, tarimDbAdapt);

                return env.addSource(testFun, "testFun", typeInfo);

            }else{
                metaClient = new TarimMetaClient("127.0.0.1", 1301);
                //todo, now the list for columns and partitionID are null
                TarimProto.PrepareScanResponse result = metaClient.preScan(100, allPartitionFlag,
                        serialize(contextBuilder.getFilters()), new ArrayList<>(), new ArrayList<>());
                if (result.getCode() != 0){
                    throw new RuntimeException("the result of preScan is incorrect!!");
                }

                TarimProto.ScanInfo res = result.getScanInfo();
                List<TarimProto.Partition> patitionList = res.getPartitionsList();

                List<ScanPartition> scanList = new ArrayList<>();
                for(TarimProto.Partition partition : patitionList){
                    List<ScanPartition.FileInfo> scanFileInfoList = new ArrayList<>();
                    List<TarimProto.FileInfo> fileInfoList = partition.getFileInfoList();


                    for (TarimProto.FileInfo fileInfo :fileInfoList){

                        scanFileInfoList.add(new ScanPartition.FileInfo(fileInfo.getPath(), fileInfo.getFormat(), fileInfo.getSizeInBytes(),
                                convertMap(fileInfo.getLowerBoundsMap()) ,convertMap(fileInfo.getUpperBoundsMap()),
                                new ArrayList<>(fileInfo.getOffsetsList()), fileInfo.getRowCount()));
                    }

                    scanList.add(new ScanPartition(partition.getPartitionID(), partition.getScanHandler(),
                            partition.getMergePolicy(), scanFileInfoList,
                            partition.getHost(), partition.getPort(), "", ""));
                }

                ((ConnectorTarimTable)tarimTable).setScanList(scanList);
                TarimFlinkInputFormat format = buildFormat();
                TarimScanContext context = contextBuilder.build();
                TypeInformation<RowData> typeInfo = FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(context.project()));


                if (!context.isStreaming()) {
                    //int parallelism = inferParallelism(format, context);
                    int parallelism = 1;
                    //ScanMergeSourceFunction function = new ScanMergeSourceFunction(format, typeInfo);
                    //String mergeFunctionName = String.format("Iceberg table (%s) scan merge", tarimTable);
                    //return env.addSource(function, mergeFunctionName, typeInfo).setParallelism(parallelism);
                    return env.createInput(format, typeInfo).setParallelism(parallelism);
                } else {
                    //todo
                    //StreamingMonitorFunction function = new StreamingMonitorFunction(tableLoader, context);

                    String monitorFunctionName = String.format("Iceberg table (%s) monitor", tarimTable);
                    String readerOperatorName = String.format("Iceberg table (%s) reader", tarimTable);
                    System.out.println("monitorFunctionName" + monitorFunctionName);
                    //return env.addSource(function, monitorFunctionName)
                    //        .transform(readerOperatorName, typeInfo, StreamingReaderOperator.factory(format));
                    return null;
                }
            }
        }

        private Map<Integer, byte[]> convertMap(Map<Integer, ByteString> srcMap){

            Map<Integer, byte[]> dstMap = new HashMap<>();
            for(Map.Entry<Integer, ByteString> entry: srcMap.entrySet()){
                dstMap.put(entry.getKey(), entry.getValue().toByteArray());
            }
            return dstMap;
        }

        public TarimFlinkInputFormat buildFormat() {
            Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

            Schema icebergSchema;
            FileIO io;
            EncryptionManager encryption;
            if (icebergTable == null) {
                // load required fields by table loader.
                tableLoader.open();
                try (TableLoader loader = tableLoader) {
                    icebergTable = loader.loadTable();
                    icebergSchema = icebergTable.schema();
                    io = icebergTable.io();
                    encryption = icebergTable.encryption();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                icebergSchema = icebergTable.schema();
                io = icebergTable.io();
                encryption = icebergTable.encryption();
            }

            if (projectedSchema == null) {
                contextBuilder.project(icebergSchema);
            } else {
                contextBuilder.project(FlinkSchemaUtil.convert(icebergSchema, projectedSchema));
            }

            //iceberg table to read parquet data
            //tarim table to load delta data
            return new TarimFlinkInputFormat(tarimDbAdapt, tarimTable, icebergTable, tarimTable.schema(), io, encryption, contextBuilder.build());
        }

        int inferParallelism(FlinkInputFormat format, TarimScanContext context) {
            int parallelism = readableConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
            if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM)) {
                int maxInferParallelism = readableConfig.get(FlinkConfigOptions
                        .TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX);
                Preconditions.checkState(maxInferParallelism >= 1,
                        FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX.key() + " cannot be less than 1");
                int splitNum;
                try {
                    FlinkInputSplit[] splits = format.createInputSplits(0);
                    splitNum = splits.length;
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to create iceberg input splits for table: " + tarimTable, e);
                }

                parallelism = Math.min(splitNum, maxInferParallelism);
            }

            if (context.limit() > 0) {
                int limit = context.limit() >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) context.limit();
                parallelism = Math.min(parallelism, limit);
            }

            // parallelism must be positive.
            parallelism = Math.max(1, parallelism);
            return parallelism;
        }

    }
    public static boolean isBounded(Map<String, String> properties) {
        return !TarimScanContext.builder().fromProperties(properties).build().isStreaming();
    }

    static String getPartitionID(ConnectorTarimTable table, String partitionStr){
        return String.format("%s=%s"
                ,table.getPartitionKey().get(0)
                ,partitionStr);
    }
}
