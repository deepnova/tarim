package org.deepexi.source;

import org.apache.iceberg.*;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

import java.util.List;

public class TarimFileScanTask implements FileScanTask {
    private final DataFile file;
    private final String schemaString;
    private final String specString;
    private PartitionSpec spec = null;
    public TarimFileScanTask(DataFile file, PartitionSpec spec, String schemaString, String specString) {
        this.file = file;
        this.spec = spec;
        this.schemaString = schemaString;
        this.specString = specString;
    }
    @Override
    public DataFile file() {
        return null;
    }

    @Override
    public List<DeleteFile> deletes() {
        return null;
    }

    @Override
    public PartitionSpec spec() {
        if (spec == null) {
            this.spec = PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaString), specString);
        }
        return spec;
    }

    @Override
    public long start() {
        return 0L;
    }

    @Override
    public long length() {
        return file.fileSizeInBytes();
    }

    @Override
    public Expression residual() {
        return null;
    }

    @Override
    public Iterable<FileScanTask> split(long l) {
        return ImmutableList.of(this);
    }
}
