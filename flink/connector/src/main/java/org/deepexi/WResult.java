package org.deepexi;

import java.io.Serializable;
import java.util.List;

public class WResult implements Serializable {
    public WResult(){}
    private List<byte[]> addDataList;

    public WResult(List<byte[]> addDataList) {
        this.addDataList = addDataList;
    }

    public List<byte[]> getDataList(){
        return addDataList;
    }

}
