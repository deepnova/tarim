package org.deepexi;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TarimDbAdapt {
    //temp members
    private static final Logger LOG = LoggerFactory.getLogger(TarimDbAdapt.class);
    private transient int subTaskId;
    private transient int attemptId;

    private transient Table table;


    public TarimDbAdapt(int subTaskId, int attemptId, Table table){
        this.subTaskId = subTaskId;
        this.attemptId = attemptId;
        this.table = table;
    }

    public <T> void writeData(List<String> element) {
        //todo, send data to trimDB
        LOG.info("writeData to tarimDB");
        return;
    }

    /*
    public StreamRecord<WResult> complete() {

        //todo , return to committed files operator
        return;
    }
    */
    public void endData(){
        //todo nodify the ending, send to tarimDB
        LOG.info("send endData to trimDB");
        return;
    };

    public boolean doCheckponit(long checkpointId){
        //todo ,send to tarimDB
        LOG.info("send doCheckponit to trimDB");
        return true;
    }

    public Long getLastommittedCheckpointId(){
        //todo, send to tarimDB
        LOG.info("send getLastommittedCheckpointId to trimDB");
        return -1L;
    }
}
