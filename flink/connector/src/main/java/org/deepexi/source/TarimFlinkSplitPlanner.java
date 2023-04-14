package org.deepexi.source;

import org.apache.commons.collections.list.TransformedList;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;

import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.deepexi.ConnectorTarimTable;

import java.util.*;

public class TarimFlinkSplitPlanner {
    private TarimFlinkSplitPlanner() {
    }

    //iceberg table to read parquet data
    //tarim table to load delta data
    static TarimFlinkInputSplit[] planInputSplits(Table tarimTable, Table icebergTable, TarimScanContext context) {
        RowType rowType = FlinkSchemaUtil.convert(tarimTable.schema());

        if(context.datafileFromIceberg()){
            //tmp: get the split for test
            List<ScanPartition> partitionsList = ((ConnectorTarimTable) tarimTable).getScanList();
            List<String> partitionList = new ArrayList<>();

            for(ScanPartition partition: partitionsList){
                partitionList.add(partition.getPartitionID());
            }

            List<FileScanTask> tasks = Lists.newArrayList(
                    icebergTable.newScan().planFiles());

//            Iterable<FileScanTask> splitTasks = FluentIterable
//                    .from(tasks)
//                    .transformAndConcat(input -> input.split(TableProperties.METADATA_SPLIT_SIZE_DEFAULT));
            HashMap<String, Integer> map = new HashMap<>();
            Object test = null;
            int index = 0;

            List<TarimCombinedScanTask> scans = new ArrayList<>();

            for (FileScanTask task : tasks){
                PartitionSpec sp = icebergTable.spec();

                // List<PartitionField> tt = load.spec().getFieldsBySourceId(1);


                List<PartitionField> field = sp.fields();
                Transform<?, ?> trans = field.get(0).transform();
                String name = trans.getClass().getName();
                if (name.equals("org.apache.iceberg.transforms.Dates")){
                    test = task.file().partition().get(0, Integer.class).toString();
                }else if (name.equals("org.apache.iceberg.transforms.Identity")){
                    test = task.file().partition().get(0, String.class);
                }

                //test = task.file().partition().get(0, String.class);


                for (int i =0; i < partitionList.size(); i++) {
                    String partitionID = partitionList.get(i);

                    if (test.equals(partitionID)){
                        if (!map.containsKey(partitionID)){

                            map.put(partitionID, index);
                        }
                        scans.get(map.get(partitionID)).addTask(task);
                        //scans.get(map.get(trunkId)).setTrunk(trunkId);
                        //scans.get(map.get(trunkId)).setScanHandle(partitionsList.get(i).scanHandler);
                    }
                }
            }

            TarimFlinkInputSplit[] splits = new TarimFlinkInputSplit[scans.size()];
            for (int i = 0; i < scans.size(); i++) {
                splits[i] = new TarimFlinkInputSplit(i, scans.get(i));
            }
            return splits;
        }else{
            //todo get the file From tarimDB
            List<ScanPartition> partitionsList = ((ConnectorTarimTable) tarimTable).getScanList();
            List<TarimCombinedScanTask> scans = new ArrayList<>();
            List<TarimFileScanTask> fileScanList = new ArrayList<>();

            //todo ? the parameters of the Metrics
            for (ScanPartition partition : partitionsList){
                int id = 0;
                long planTotalSize = 0;
                int fileNumber = partition.getFileInfoList().size();
                int partitionUpperType = 0;
                int partitionLowerType = 0;
                String upperBound = "";
                String lowerBound = "";

                for (int index = 0; index < fileNumber; index++){
                    id = 0;
                    ScanPartition.FileInfo fileInfo = partition.getFileInfoList().get(index);

                    if (planTotalSize > 512 * 1000 * 1000){
                        scans.add(new TarimCombinedScanTask(
                                ((ConnectorTarimTable) tarimTable).getTableId(),
                                rowType,
                                ((ConnectorTarimTable) tarimTable).getSchemaJson(),
                                partition.getPartitionID(),
                                0,
                                partition.getHost(),
                                partition.getPort(),
                                new ArrayList<>(fileScanList),
                                id,
                                lowerBound,
                                upperBound,
                                partitionLowerType,
                                partitionUpperType));
                        fileScanList.clear();
                        planTotalSize = 0;
                        id++;
                    }

                    DataFile datafile = DataFiles.builder(tarimTable.spec())
                            .withFormat(fileInfo.format)
                            .withPath(fileInfo.path)
                            .withPartition(null)
                            .withFileSizeInBytes(fileInfo.sizeInBytes)
                            .withMetrics(new Metrics(fileInfo.rowCounts, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()))
                            .withSplitOffsets(fileInfo.offsets)
                            .withSortOrder(null)
                            .build();

                    TarimFileScanTask fileScanTask = new TarimFileScanTask(datafile, tarimTable.spec(), null, null);
                    fileScanList.add(fileScanTask);

                    if (id == 0) {
                        //the first plan
                        if (partition.getPartitionLowerBound().equals("")) {
                            partitionLowerType = 0;
                        } else {
                            partitionLowerType = 1;
                        }
                    }

                    if (planTotalSize == 0){
                        //the first range in each plan
                        //index for the primary
                        lowerBound = fileInfo.lowerBounds.get(0);
                    }
                    upperBound = fileInfo.upperBounds.get(0);
                    partitionUpperType= 2;

                    planTotalSize += fileInfo.sizeInBytes;

                    if (index == (fileNumber - 1)){
                        //the first plan
                        if (partition.getPartitionUpperBound().equals("")){
                            partitionUpperType = 0;
                        }else{
                            partitionUpperType = 1;
                        }

                        scans.add(new TarimCombinedScanTask(
                                ((ConnectorTarimTable) tarimTable).getTableId(),
                                rowType,
                                ((ConnectorTarimTable) tarimTable).getSchemaJson(),
                                partition.getPartitionID(),
                                0,
                                partition.getHost(),
                                partition.getPort(),
                                new ArrayList<>(fileScanList),
                                id,
                                lowerBound,
                                upperBound,
                                partitionLowerType,
                                partitionUpperType));

                        fileScanList.clear();
                        planTotalSize = 0;
                    }
                }
            }

            TarimFlinkInputSplit[] splits = new TarimFlinkInputSplit[scans.size()];
            for (int i = 0; i < scans.size(); i++) {
                splits[i] = new TarimFlinkInputSplit(i, scans.get(i));
            }
            return splits;
        }
    }
    static CloseableIterable<CombinedScanTask> planTasks(Table table, TarimScanContext context) {
        TableScan scan = table
                .newScan()
                .caseSensitive(context.caseSensitive())
                .project(context.project());

        if (context.includeColumnStats()) {
            scan = scan.includeColumnStats();
        }

        if (context.snapshotId() != null) {
            scan = scan.useSnapshot(context.snapshotId());
        }

        if (context.asOfTimestamp() != null) {
            scan = scan.asOfTime(context.asOfTimestamp());
        }

        if (context.isStreaming()) {
            scan = scan.streaming(true);
        }

        if (context.startSnapshotId() != null) {
            if (context.endSnapshotId() != null) {
                scan = scan.appendsBetween(context.startSnapshotId(), context.endSnapshotId());
            } else {
                scan = scan.appendsAfter(context.startSnapshotId());
            }
        }

        if (context.splitSize() != null) {
            scan = scan.option(TableProperties.SPLIT_SIZE, context.splitSize().toString());
        }

        if (context.splitLookback() != null) {
            scan = scan.option(TableProperties.SPLIT_LOOKBACK, context.splitLookback().toString());
        }

        if (context.splitOpenFileCost() != null) {
            scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, context.splitOpenFileCost().toString());
        }

        if (context.filters() != null) {
            for (Expression filter : context.filters()) {
                scan = scan.filter(filter);
            }
        }

        return scan.planTasks();
    }
}
