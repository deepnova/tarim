package org.deepexi.source;

import java.util.List;

public class ScanPartition {
    String partitionID;
    long scanHandler;
    int mergePolicy;
    List<String> filePath;
    public ScanPartition(String partitionID, long scanHandler, int mergePolicy, List<String> filePath){
        this.partitionID = partitionID;
        this.scanHandler = scanHandler;
        this.mergePolicy = mergePolicy;
        this.filePath = filePath;
    }

    String getPartitionID(){
        return this.partitionID;
    }

    long getScanHandler(){
        return this.scanHandler;
    }

    List<String> getFilePath(){
        return this.filePath;
    }
}
