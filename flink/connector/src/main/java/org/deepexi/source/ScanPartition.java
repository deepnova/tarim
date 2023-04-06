package org.deepexi.source;

import java.util.List;

public class ScanPartition {
    String partitionID;
    long scanHandler;
    int mergePolicy;
    List<String> filePath;
    String host;
    int port;

    public ScanPartition(String partitionID, long scanHandler, int mergePolicy, List<String> filePath, String host, int port){
        this.partitionID = partitionID;
        this.scanHandler = scanHandler;
        this.mergePolicy = mergePolicy;
        this.filePath = filePath;
        this.host = host;
        this.port = port;
    }

    String getPartitionID(){
        return this.partitionID;
    }

    long getScanHandler(){
        return this.scanHandler;
    }
    int getPort(){
        return this.port;
    }
    String getHost(){
        return this.host;
    }
    List<String> getFilePath(){
        return this.filePath;
    }
}
