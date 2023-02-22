package org.deepexi.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.io.FileIO;

public class TarimTableOperations extends HadoopTableOperations implements TableOperations {

    public long newSnapshotId;
    TarimTableOperations(Path location, FileIO fileIO, Configuration conf, LockManager lockManager, Long newSnapshotId ){
        super(location, fileIO, conf, lockManager);
        this.newSnapshotId = newSnapshotId;
    }
    @Override
    public long newSnapshotId(){
        return newSnapshotId;
    }
}