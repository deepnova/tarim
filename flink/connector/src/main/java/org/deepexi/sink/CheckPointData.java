package org.deepexi.sink;

import java.io.Serializable;

//data for checkpoint to recover
public class CheckPointData implements Serializable {
    private String host;
    private int port;
    private String partitionID;
    private byte[] data;

    CheckPointData(String host, int port, String partitionID, byte[] data){
        this.host = host;
        this.port = port;
        this.partitionID = partitionID;
        this.data = data;
    }

    String getHost(){
        return this.host;
    }

    int getPort(){
        return this.port;
    }

    String getPartitionID(){
        return this.partitionID;
    }

    byte[] getData(){
        return this.data;
    }
}
