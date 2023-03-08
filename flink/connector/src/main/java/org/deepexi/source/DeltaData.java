package org.deepexi.source;

import org.apache.flink.table.data.RowData;

public class DeltaData {
    private boolean matchFlag = false;

    private final RowData data;

    public DeltaData(RowData data){
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
}
