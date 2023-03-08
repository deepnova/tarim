package org.deepexi.source;

import org.apache.flink.table.data.RowData;

public class DeltaData {
    private boolean matchFlag = false;

    private RowData data;

    public DeltaData(RowData data){
        this.data = data;
    }

    public void setMatchFlag(boolean matchFlag){
        this.matchFlag = matchFlag;
    }

    public RowData getDatas(){
        return this.data;
    }

    public boolean getMatchFlag(){
        return this.matchFlag;
    }
}
