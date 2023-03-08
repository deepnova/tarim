package org.deepexi.source;

import org.apache.iceberg.*;
import org.apache.iceberg.expressions.Expression;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;

import java.util.*;

public class TarimFlinkSplitPlanner {
    private TarimFlinkSplitPlanner() {
    }

    static TarimFlinkInputSplit[] planInputSplits(Table table, TarimScanContext context) {

        if(context.datafileFromIceberg()){
            //tmp: get the split for test
            List<String> trunkList = new ArrayList<>();
            trunkList.add("d1");
            trunkList.add("d3");
            trunkList.add("d5");
            List<FileScanTask> tasks = Lists.newArrayList(
                    table.newScan().planFiles());

            HashMap<String, Integer> map = new HashMap<>();
            Object test = null;
            int index = 0;

            List<TarimCombinedScanTask> scans = new ArrayList<>();

            for (FileScanTask task : tasks){
                PartitionSpec sp = table.spec();

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

                for (int i =0; i < trunkList.size(); i++) {

                    String trunkId = trunkList.get(i);

                    if (test.equals(trunkId)){
                        if (!map.containsKey(trunkId)){

                            map.put(trunkId, index);

                            scans.add(new TarimCombinedScanTask(trunkId, new ArrayList<>()));
                            index++;
                        }
                        scans.get(map.get(trunkId)).addTask(task);
                        scans.get(map.get(trunkId)).setTrunk(trunkId);
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
            return new TarimFlinkInputSplit[0];
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
