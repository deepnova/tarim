package org.deepexi;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.deepexi.source.DeltaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import static org.apache.flink.table.data.StringData.fromString;

public class TarimDbAdapt implements Serializable {
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
        LOG.info("send checkponit to trimDB");
        return true;
    }

    public Long getLastommittedCheckpointId(){
        //todo, send to tarimDB
        LOG.info("send getLastommittedCheckpointId to trimDB");
        return -1L;
    }

    public List<DeltaData> getDeltaData(String trunkId) throws InterruptedException {
        //todo, get delta data from tarimDB
        LOG.info("getDeltaData to tarimDB");
        List<DeltaData> datalist = new ArrayList<>();

        //simulate some data
        if (trunkId.equals("d1")){
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1222"), 2)));

            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10000)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10001)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10002)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10003)));

        }else if(trunkId.equals("d3")){
            datalist.add(new DeltaData(GenericRowData.of(fromString("33333"), 3)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30000)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30001)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30002)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30003)));
        }else{
            return datalist;
        }

        Thread.sleep(3000);
        return datalist;
    }
}
