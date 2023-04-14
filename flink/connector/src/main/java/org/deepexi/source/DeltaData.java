package org.deepexi.source;

import org.apache.flink.table.data.RowData;

public class DeltaData {
    private boolean matchFlag = false;

    private int op;

    private final RowData data;

    public DeltaData(int op, RowData data){
        this.op = op;
        this.data = data;
    }

    public void setMatchFlag(boolean matchFlag){
        this.matchFlag = matchFlag;
    }

    public RowData getData(){
        return this.data;
    }

    public boolean getMatchFlag(){
        return this.matchFlag;
    }

    public void setOp(int op){
        this.op = op;
    }
    public int getOp(){
        return this.op;
    }
}
