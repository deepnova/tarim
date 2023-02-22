package org.deepexi.sink;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.iceberg.Table;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.deepexi.TarimDbAdapt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TarimStreamWriter<T> extends AbstractStreamOperator<Long>
        implements OneInputStreamOperator<T, Long>, BoundedOneInput {
    private static final Logger LOG = LoggerFactory.getLogger(TarimStreamWriter.class);
    private static final long serialVersionUID = 1L;

    private final String fullTableName;

    private transient int subTaskId;
    private transient int attemptId;

    private transient Table table;
    private TarimDbAdapt dbAdapter;
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
        //writer.write(element.getValue());
        dbAdapter.writeData(element);
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

        output.collect(dbAdapter.complete());
    }
}
