package org.deepexi.source;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ScanPartition implements Serializable {
    private String partitionID;
    private long scanHandler;
    private int mergePolicy;
    private List<String> filePath;
    private String host;
    private int port;

    private String partitionLowerBound;
    private String partitionUpperBound;

    private List<FileInfo> fileInfoList;

    public ScanPartition(String partitionID, long scanHandler, int mergePolicy, List<FileInfo> fileInfoList, String host, int port,
                         String partitionLowerBound, String partitionUpperBound){
        this.partitionID = partitionID;
        this.scanHandler = scanHandler;
        this.mergePolicy = mergePolicy;
        this.fileInfoList = fileInfoList;
        this.host = host;
        this.port = port;
        this.partitionLowerBound = partitionLowerBound;  //don't support now
        this.partitionUpperBound = partitionUpperBound;  //don't support now
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

    List<FileInfo> getFileInfoList(){
        return this.fileInfoList;
    }
    String getPartitionLowerBound(){
        return this.partitionLowerBound;
    }
    String getPartitionUpperBound(){
        return this.partitionUpperBound;
    }

    public static class FileInfo implements Serializable{
        public String path;
        public String format;
        public long sizeInBytes;

        public Map<Integer, String> lowerBounds;
        public Map<Integer, String> upperBounds;
        public List<Long> offsets;
        public long rowCounts;

        public FileInfo(String path, String format, long sizeInBytes, Map<Integer, String> lowerBounds, Map<Integer, String> upperBounds,
                        List<Long> offsets, long rowCounts){
            this.path = path;
            this.format = format;
            this.sizeInBytes = sizeInBytes;
            this.lowerBounds = lowerBounds;
            this.upperBounds = upperBounds;
            this.offsets = offsets;
            this.rowCounts = rowCounts;
        }
    }

}
