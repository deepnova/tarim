package org.deepexi.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
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
import java.util.List;
import java.util.Map;
import org.deepexi.TarimDbAdapt;
public class TarimSource {
    private TarimSource() {
    }

    public static Builder forRowData() {
        return new Builder();
    }

    public static class Builder {
        private StreamExecutionEnvironment env;
        private Table table;
        private TableLoader tableLoader;
        private TableSchema projectedSchema;
        private ReadableConfig readableConfig = new Configuration();
        public TarimDbAdapt tarimDbAdapt = new TarimDbAdapt(0, 0, null);
        private final TarimScanContext.Builder contextBuilder = TarimScanContext.builder();

        public Builder table(Table newTable) {
            this.table = newTable;
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
        public DataStream<RowData> build() {
            Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");

            //todo, do PrepareScan to TarimDB, syn msg, receive the trunk information
            TarimFlinkInputFormat format = buildFormat();

            TarimScanContext context = contextBuilder.build();
            TypeInformation<RowData> typeInfo = FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(context.project()));


            if (!context.isStreaming()) {
                //int parallelism = inferParallelism(format, context);
                int parallelism = 1;
                return env.createInput(format, typeInfo).setParallelism(parallelism);
            } else {
                //todo
                //StreamingMonitorFunction function = new StreamingMonitorFunction(tableLoader, context);

                String monitorFunctionName = String.format("Iceberg table (%s) monitor", table);
                String readerOperatorName = String.format("Iceberg table (%s) reader", table);
                System.out.println("monitorFunctionName" + monitorFunctionName);
                //return env.addSource(function, monitorFunctionName)
                //        .transform(readerOperatorName, typeInfo, StreamingReaderOperator.factory(format));
                return null;
            }
        }

        public TarimFlinkInputFormat buildFormat() {
            Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

            Schema icebergSchema;
            FileIO io;
            EncryptionManager encryption;
            if (table == null) {
                // load required fields by table loader.
                tableLoader.open();
                try (TableLoader loader = tableLoader) {
                    table = loader.loadTable();
                    icebergSchema = table.schema();
                    io = table.io();
                    encryption = table.encryption();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                icebergSchema = table.schema();
                io = table.io();
                encryption = table.encryption();
            }

            if (projectedSchema == null) {
                contextBuilder.project(icebergSchema);
            } else {
                contextBuilder.project(FlinkSchemaUtil.convert(icebergSchema, projectedSchema));
            }

            //todo, tmp for the table

            return new TarimFlinkInputFormat(tarimDbAdapt, table, icebergSchema, io, encryption, contextBuilder.build());
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
                    throw new UncheckedIOException("Failed to create iceberg input splits for table: " + table, e);
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

}
