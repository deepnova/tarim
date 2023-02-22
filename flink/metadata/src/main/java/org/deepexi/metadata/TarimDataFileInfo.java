package org.deepexi.metadata;

import org.apache.iceberg.*;
import org.apache.thrift.meta_data.SetMetaData;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TarimDataFileInfo {
    public String filePath;
    public FileFormat format;
    public String location;
    public Long length;
    public List<Long> splitOffsets;
    public SortOrder sortOrder;
    //metrics info
    public Metrics metrics;

    public TarimDataFileInfo(FileFormat format, String location, Long length, List<Long> splitOffsets,
                      SortOrder sortOrder){
        this.format = format;
        this.location = location;
        this.length = length;
        this.splitOffsets = splitOffsets;
        this.sortOrder = sortOrder;
    }

    public void SetMetrics(Long rowCount, Map<Integer, Long> columnSizes, Map<Integer, Long> valueCounts, Map<Integer, Long> nullValueCounts, Map<Integer, Long> nanValueCounts, Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds){
        this.metrics = new Metrics(rowCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
    }

}
