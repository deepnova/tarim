package org.deepexi.source;

import org.apache.iceberg.*;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

import java.util.List;

public class TarimFileScanTask implements FileScanTask {
    private final DataFile file;
    private final DeleteFile[] deletes;
    private final String schemaString;
    private final String specString;
    private PartitionSpec spec = null;
    public TarimFileScanTask(DataFile file, DeleteFile[] deletes, PartitionSpec spec, String schemaString, String specString) {
        this.file = file;
        this.deletes = deletes != null ? deletes : new DeleteFile[0];
        this.spec = spec;
        this.schemaString = schemaString;
        this.specString = specString;
    }
    @Override
    public DataFile file() {
        return file;
    }

    @Override
    public List<DeleteFile> deletes() {
        return ImmutableList.copyOf(deletes);
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
