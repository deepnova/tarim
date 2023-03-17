package org.deepexi.sink;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import org.apache.iceberg.flink.RowDataWrapper;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.deepexi.ConnectorTarimTable;
import org.deepexi.TarimDbAdapt;
import org.deepexi.WResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.deepexi.sink.TarimSink.Builder.toFlinkRowType;

public class TarimStreamWriter<T> extends AbstractStreamOperator<WResult>
        implements OneInputStreamOperator<T, WResult>, BoundedOneInput {
    private static final Logger LOG = LoggerFactory.getLogger(TarimStreamWriter.class);
    private static final long serialVersionUID = 1L;

    private final String fullTableName;

    private transient int subTaskId;
    private transient int attemptId;

    private transient Table table;
    private TarimDbAdapt dbAdapter;
    public List<String> dataList = new ArrayList<>();
    public List<String> completeList = new ArrayList<>();
    public int batchNum = 10;
    public int dataIndex = 0;

    TarimStreamWriter(String fullTableName, Table table) {
        this.fullTableName = fullTableName;
        this.table = table;
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }


    @Override
    public void open() {
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.attemptId = getRuntimeContext().getAttemptNumber();

        this.dbAdapter = new TarimDbAdapt(subTaskId, attemptId, table);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // close all open files and emit files to downstream committer operator
        flush();
        //this.writer = taskWriterFactory.create();
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        LOG.info("stream-processElement" + element.getValue().toString());

        final Map<PartitionKey, String> writers = Maps.newHashMap();


        //writer.write(element.getValue());
        if (element != null){
            PartitionSpec partitionSpec = table.spec();
            Schema schema = table.schema();
            TableSchema flinkSchema =((ConnectorTarimTable)table).getFlinkSchema();
            PartitionKey partitionKey = new PartitionKey(partitionSpec,schema);

            RowType flinkRowType = toFlinkRowType(schema, flinkSchema);
            RowDataWrapper wrapper = new RowDataWrapper(flinkRowType, schema.asStruct());

            partitionKey.partition(wrapper.wrap((RowData) element.getValue()));

            String partitionForChunk = partitionSpec.partitionToPath(partitionKey);

            System.out.println("chunk:" + partitionForChunk);
            dataList.add(element.getValue().toString());
            dataIndex++;

            if (dataIndex >= batchNum){
                dbAdapter.writeData(dataList);
                completeList.addAll(dataList);
                dataList.clear();
                dataIndex = 0;
            }
        }

    }

    @Override
    public void close() throws Exception {
        return;
    }

    @Override
    public void endInput() throws IOException {
        // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the remaining
        // completed files to downstream before closing the writer so that we won't miss any of them.
        flush();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("table_name", fullTableName)
                .add("subtask_id", subTaskId)
                .add("attempt_id", attemptId)
                .toString();
    }

    private void flush() throws IOException {
        LOG.info("stream-flush...");

        if (dataList.size() > 0){
            completeList.addAll(dataList);
            dbAdapter.writeData(dataList);
            dataList.clear();

            dataIndex = 0;
        }else{
            LOG.info("the list is empty");
        }
        WResult wr = new WResult(completeList);
        StreamRecord<WResult> streamRecord =new StreamRecord<>(wr);
        output.collect(streamRecord);
    }
}
