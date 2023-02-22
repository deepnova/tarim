package org.deepexi;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;

public class TarimDbAdapt {
    //temp members
    private transient int subTaskId;
    private transient int attemptId;

    private transient Table table;

    public TarimDbAdapt(int subTaskId, int attemptId, Table table){
        this.subTaskId = subTaskId;
        this.attemptId = attemptId;
        this.table = table;
    }

    public <T> void writeData(StreamRecord<T> element) {
        //todo, send data to trimDB
        return;
    }

    public StreamRecord<Long> complete() {

        //todo , return to committed files operator
        StreamRecord<Long> streamRecord =new StreamRecord<>(9L);
        return streamRecord;
    }

    public void endData(){
        //todo nodify the ending, send to tarimDB
        return;
    };

    public boolean doCheckponit(long checkpointId){
        //todo ,send to tarimDB
        return true;
    }

    public Long getLastommittedCheckpointId(){
        //todo, send to tarimDB
        return -1L;
    }
}
