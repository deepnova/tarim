package org.deepexi.metadata;

//import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.hadoop.*;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LockManagers;
import org.deepexi.metadata.TarimDataFileInfo;
import org.deepexi.metadata.TarimMetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

//@Slf4j
public class  TestWrite {
/*
    static {
        String home = "C:\\work\\hadoop";
        System.setProperty("hadoop.home.dir", home);
    }
 */
/*
    public static class TarimCatalogUtil extends CatalogUtil{

        public static Catalog loadCatalog(String impl, String catalogName, Map<String, String> properties, Object hadoopConf) {
            Preconditions.checkNotNull(impl, "Cannot initialize custom Catalog, impl class name is null");

            DynConstructors.Ctor ctor;
            try {
                ctor = DynConstructors.builder(Catalog.class).impl(impl, new Class[0]).buildChecked();
            } catch (NoSuchMethodException var8) {
                throw new IllegalArgumentException(String.format("Cannot initialize Catalog implementation %s: %s", impl, var8.getMessage()), var8);
            }

            Catalog catalog;
            try {
                catalog = (Catalog)ctor.newInstance(new Object[0]);
            } catch (ClassCastException var7) {
                throw new IllegalArgumentException(String.format("Cannot initialize Catalog, %s does not implement Catalog.", impl), var7);
            }

            configureHadoopConf(catalog, hadoopConf);
            catalog.initialize(catalogName, properties);
            return catalog;
        }

    }
*/


    public static void main(String[] args) throws Exception {
        /*
        try {
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();

            StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            bsEnv.setParallelism(1);
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            bsEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            bsEnv.enableCheckpointing(120*1000);
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, settings);

            test111(bsEnv, tableEnv);

        } catch (Exception e) {
            log.error("", e);
        } finally {
            System.out.println("=========结束=========");
        }
        */
        String CATALOG_NAME = "hadoop_catalog";
        String DATABASE_NAME = "default_db";
        String TABLE_NAME = "new_table4";
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", "hdfs://10.201.0.119:9000/wpf3");
        Long newSnapshotId = 400000002L;
        //properties.put("warehouse", "hdfs://10.201.0.119:9000/wpf3");

        //CatalogLoader loader = CatalogLoader.hadoop(CATALOG_NAME, new Configuration(), properties);

        //NewCatalogLoader loader = NewCatalogLoader.tarim(CATALOG_NAME, new Configuration(), properties);
        //TarimCatalog tarimCatalog = new TarimCatalog
        //TarimCatalog tarimCatalog = (TarimCatalog) loader.loadCatalog();
        //TarimTable tarimTable = tarimCatalog.loadTable(TableIdentifier.of(DATABASE_NAME, "new_table2"));

        TarimMetaData metaData = new TarimMetaData(CATALOG_NAME, DATABASE_NAME, TABLE_NAME, new Configuration(), properties, newSnapshotId);
        if (metaData == null){
            return;
        }
        int specId = 0;
        String location = "/usr/local/wpf/tmp/00000-1-e45ba23c-7420-408e-b968-58acdb7062a1-00001.parquet";
        List<Long> splitOffsets = new ArrayList<Long>(1);
        splitOffsets.add(4L);
        SortOrder sortOrder = null;
        Long length = 932L;
        TarimDataFileInfo fileInfo = new TarimDataFileInfo(FileFormat.PARQUET, location, length, splitOffsets, sortOrder);

        Long rowCount = 1L;
        Map<Integer, Long> columnSizes = new HashMap<Integer, Long>();
        columnSizes.put(1,54L);
        columnSizes.put(2,47L);
        columnSizes.put(3,54L);

        Map<Integer, Long> valueCounts = new HashMap<Integer, Long>();
        valueCounts.put(1,1L);
        valueCounts.put(2,1L);
        valueCounts.put(3,1L);

        Map<Integer, Long> nullValueCounts = new HashMap<Integer, Long>();
        nullValueCounts.put(1,0L);
        nullValueCounts.put(2,0L);
        nullValueCounts.put(3,0L);

        Map<Integer, Long> nanValueCounts = new HashMap<Integer, Long>();

        Map<Integer, ByteBuffer> lowerBounds = new HashMap<Integer, ByteBuffer>();
        ByteBuffer a1 = ByteBuffer.allocate(3);
        a1.put((byte)'d');
        a1.put((byte)'1');
        a1.put((byte)'3');
        lowerBounds.put(1, a1);

        ByteBuffer a2 = ByteBuffer.allocate(4);
        a2.put((byte)'q');
        a2.put((byte)'0');
        a2.put((byte)'0');
        a2.put((byte)'0');
        lowerBounds.put(2, a2);

        ByteBuffer a3 = ByteBuffer.allocate(3);
        a3.put((byte)'1');
        a3.put((byte)'1');
        a3.put((byte)'3');
        lowerBounds.put(3, a3);

        Map<Integer, ByteBuffer> upperBounds = new HashMap<Integer, ByteBuffer>();
        upperBounds = lowerBounds;

        fileInfo.SetMetrics(rowCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);

        List<DataFile> datafiles = new ArrayList<>();
        DataFile datafile = metaData.buildDataFile(fileInfo);
        datafiles.add(datafile);

        metaData.buildMetaData(datafiles);
        //datafiles.add(datafile);

        //ManifestFile = writeManifest(create(tarimTable.io()), spec, 0L, datafiles);

        //AppendFiles appendFiles = tarimTable.newAppend().appendFile(datafile);
/*
        WriteResult ret = WriteResult.builder().addDataFiles(datafiles).build();

        NavigableMap<Long, WriteResult> pendingResults = new TreeMap();

        pendingResults.put(0L, ret);
        pendingResults.put(1L, ret);

        int numFiles = 0;
        for (WriteResult result : pendingResults.values()){
            numFiles += result.dataFiles().length;

            Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        }
*/
        //appendFiles.commit();

        return;
    }


    static OutputFile create(FileIO io){
        String filePath =  "D:\\tarimTest\\new_table2\\test1.avro";
        return io.newOutputFile(filePath);
    }

    static ManifestFile writeManifest(OutputFile outputFile, PartitionSpec spec, Long snapshotId, List<DataFile> files) throws IOException {

        ManifestWriter<DataFile> writer = ManifestFiles.write(2, spec, outputFile, snapshotId);
        try {
            for (DataFile file : files) {
                writer.add(file);
            }
        } finally {
            writer.close();
        }

        return writer.toManifestFile();
    }
}
