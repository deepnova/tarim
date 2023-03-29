package org.deepexi.sink;

import com.deepexi.TarimMetaClient;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.*;
import org.apache.iceberg.flink.FlinkSchemaUtil;


import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import org.apache.iceberg.types.TypeUtil;

import org.deepexi.ConnectorTarimTable;
import org.deepexi.SerializableTarimTable;
import org.deepexi.WResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.iceberg.DistributionMode.NONE;
import static org.apache.iceberg.TableProperties.*;


public class TarimSink{

    private static final Logger LOG = LoggerFactory.getLogger(TarimSink.class);

    private static final String TARIM_STREAM_WRITER_NAME = TarimStreamWriter.class.getSimpleName();
    private static final String TARIM_FILES_COMMITTER_NAME = TarimFilesCommitter.class.getSimpleName();
    public static Builder forRowData(DataStream<RowData> input) {
        return new Builder().forRowData(input);
    }
    public static class Builder {
        private Table table;
        private TableSchema tableSchema;
        private boolean overwrite = false;
        private DistributionMode distributionMode = null;
        private Integer writeParallelism = null;
        private boolean upsert = false;
        private Function<String, DataStream<RowData>> inputCreator = null;
        private String uidPrefix = null;
        private List<String> equalityFieldColumns = null;
        private Builder() {
        }

        private <T> TarimSink.Builder forMapperOutputType(DataStream<T> input,
                                                          MapFunction<T, RowData> mapper,
                                                          TypeInformation<RowData> outputType) {
            this.inputCreator = newUidPrefix -> {
                if (newUidPrefix != null) {
                    return input.map(mapper, outputType)
                            .name(operatorName(newUidPrefix))
                            .uid(newUidPrefix + "-mapper");
                } else {
                    return input.map(mapper, outputType);
                }
            };
            return this;
        }
        private <T> DataStreamSink<T> chainIcebergOperators() {
            Preconditions.checkArgument(inputCreator != null,
                    "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
            Preconditions.checkNotNull(table, "Table loader shouldn't be null");

            DataStream<RowData> rowDataInput = inputCreator.apply(uidPrefix);

            // Convert the requested flink table schema to flink row type.
            RowType flinkRowType = toFlinkRowType(table.schema(), tableSchema);

            // Distribute the records from input data stream based on the write.distribution-mode.
            DataStream<RowData> distributeStream = distributeDataStream(
                    rowDataInput, table.properties(), null, null, null);

            // Add parallel writers that append rows to files
            SingleOutputStreamOperator<WResult> writerStream = appendWriter(distributeStream, flinkRowType);

            // Add single-parallelism committer that commits files
            // after successful checkpoint or end of input
            SingleOutputStreamOperator<Void> committerStream = appendCommitter(writerStream);

            // Add dummy discard sink
            return appendDummySink(committerStream);
        }
        public DataStreamSink<Void> append() {
            return chainIcebergOperators();
        }

        private String operatorName(String suffix) {
            return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
        }
        private TarimSink.Builder forRowData(DataStream<RowData> newRowDataInput) {
            this.inputCreator = ignored -> newRowDataInput;
            return this;
        }


        public TarimSink.Builder table(Table newTable) {
            this.table = newTable;
            return this;
        }
        public TarimSink.Builder tableSchema(TableSchema newTableSchema) {
            this.tableSchema = newTableSchema;
            return this;
        }

        public TarimSink.Builder overwrite(boolean newOverwrite) {
            this.overwrite = newOverwrite;
            return this;
        }


        public TarimSink.Builder distributionMode(DistributionMode mode) {
            Preconditions.checkArgument(!DistributionMode.RANGE.equals(mode),
                    "Tarim does not support 'range' write distribution mode now.");
            this.distributionMode = mode;
            return this;
        }

        public TarimSink.Builder writeParallelism(int newWriteParallelism) {
            this.writeParallelism = newWriteParallelism;
            return this;
        }

        public TarimSink.Builder upsert(boolean enabled) {
            this.upsert = enabled;
            return this;
        }
        public TarimSink.Builder uidPrefix(String newPrefix) {
            this.uidPrefix = newPrefix;
            return this;
        }

        public TarimSink.Builder equalityFieldColumns(List<String> columns) {
            this.equalityFieldColumns = columns;
            return this;
        }
        static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
            if (requestedSchema != null) {
                // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing iceberg schema.
                Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
                TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

                // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will be promoted to
                // iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT (backend by 1 'byte'), we will
                // read 4 bytes rather than 1 byte, it will mess up the byte array in BinaryRowData. So here we must use flink
                // schema.
                return (RowType) requestedSchema.toRowDataType().getLogicalType();
            } else {
                return FlinkSchemaUtil.convert(schema);
            }
        }
        private DataStream<RowData> distributeDataStream(DataStream<RowData> input,
                                                         Map<String, String> properties,
                                                         PartitionSpec partitionSpec,
                                                         Schema iSchema,
                                                         RowType flinkRowType) {
            DistributionMode writeMode;
            if (distributionMode == null) {
                // Fallback to use distribution mode parsed from table properties if don't specify in job level.
                //String modeName = PropertyUtil.propertyAsString(properties,
                //        WRITE_DISTRIBUTION_MODE,
                //        WRITE_DISTRIBUTION_MODE_NONE);

                //writeMode = DistributionMode.fromName(modeName);
                writeMode = NONE;
            } else {
                writeMode = distributionMode;
            }

            switch (writeMode) {
                case NONE:
                    return input;

                case HASH:
                    return input;
                    /*
                    if (partitionSpec.isUnpartitioned()) {
                        return input;
                    } else {
                        return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
                    }

                     */

                case RANGE:
                    LOG.warn("Fallback to use 'none' distribution mode, because {}={} is not supported in flink now",
                            WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName());
                    return input;

                default:
                    throw new RuntimeException("Unrecognized write.distribution-mode: " + writeMode);
            }
        }
        private SingleOutputStreamOperator<WResult> appendWriter(DataStream<RowData> input,
                                                                     RowType flinkRowType) {
            // Find out the equality field id list based on the user-provided equality field column names.
            List<Integer> equalityFieldIds = Lists.newArrayList();
            if (equalityFieldColumns != null && equalityFieldColumns.size() > 0) {
                for (String column : equalityFieldColumns) {
                    org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
                    Preconditions.checkNotNull(field, "Missing required equality field column '%s' in table schema %s",
                            column, table.schema());
                    equalityFieldIds.add(field.fieldId());
                }
            }

            int parallelism = writeParallelism == null ? input.getParallelism() : writeParallelism;

            TarimStreamWriter<RowData> streamWriter = createStreamWriter(table, flinkRowType, parallelism);
            SingleOutputStreamOperator<WResult> writerStream = input
                    .transform(operatorName(TARIM_STREAM_WRITER_NAME), TypeInformation.of(WResult.class), streamWriter)
                    .setParallelism(parallelism);
            if (uidPrefix != null) {
                writerStream = writerStream.uid(uidPrefix + "-writer");
            }
            return writerStream;
        }

        static TarimStreamWriter<RowData> createStreamWriter(Table table, RowType rowType,
                                                               int parallelism) {
            Preconditions.checkArgument(table != null, "Iceberg table should't be null");
            Map<String, String> props = table.properties();

            //todo fixed the host and ip now
            ((ConnectorTarimTable)table).setMetaClient(new TarimMetaClient("127.0.0.1", 1301));

            ((ConnectorTarimTable)table).metaClient.getDistribution();

            SerializableTarimTable serializableTable = (SerializableTarimTable) SerializableTarimTable.copyOf((ConnectorTarimTable)table);

            RowDataTaskWriterFactory writerFactory = new RowDataTaskWriterFactory(serializableTable, rowType, parallelism);

            return new TarimStreamWriter<>(table.name(), writerFactory);
        }

        private <T> DataStreamSink<T> appendDummySink(SingleOutputStreamOperator<Void> committerStream) {
            DataStreamSink<T> resultStream = committerStream
                    .addSink(new DiscardingSink())
                    .name(operatorName(String.format("IcebergSink %s", this.table.name())))
                    .setParallelism(1);
            if (uidPrefix != null) {
                resultStream = resultStream.uid(uidPrefix + "-dummysink");
            }
            return resultStream;
        }

        private SingleOutputStreamOperator<Void> appendCommitter(SingleOutputStreamOperator<WResult> writerStream) {
            TarimFilesCommitter filesCommitter = new TarimFilesCommitter(table);
            SingleOutputStreamOperator<Void> committerStream = writerStream
                    .transform(operatorName(TARIM_FILES_COMMITTER_NAME), Types.VOID, filesCommitter)
                    .setParallelism(1)
                    .setMaxParallelism(1);
            if (uidPrefix != null) {
                committerStream = committerStream.uid(uidPrefix + "-committer");
            }
            return committerStream;
        }
    }
}
