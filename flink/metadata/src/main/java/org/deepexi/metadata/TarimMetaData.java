package org.deepexi.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Map;

public class TarimMetaData {
    //public List<TarimDataFileInfo> dataFilesInfo;
    public TarimTable table;

    public TarimMetaData(String catalogName, String dbName, String tableName, Configuration hadoopConf, Map<String, String> properties, Long newSnapshotId){
        NewCatalogLoader loader = NewCatalogLoader.tarim(catalogName, new Configuration(), properties);
        //TarimCatalog tarimCatalog = new TarimCatalog
        TarimCatalog tarimCatalog = (TarimCatalog) loader.loadCatalog();
        table = tarimCatalog.loadTarimTable(TableIdentifier.of(dbName, tableName), newSnapshotId);
    }

    public DataFile buildDataFile(TarimDataFileInfo fileInfo){
        return DataFiles.builder(table.spec()).withFormat(fileInfo.format).withPath(fileInfo.location).withPartition(null).withFileSizeInBytes(fileInfo.length).withMetrics(fileInfo.metrics).withSplitOffsets(fileInfo.splitOffsets).withSortOrder(fileInfo.sortOrder).build();
    }

    public void buildMetaData(List<DataFile> datafiles){

        for(DataFile file: datafiles) {
            table.newAppend().appendFile(file).commit();
        }
    }
}
