package org.deepexi.source;

import org.apache.flink.core.io.InputSplit;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;


public class TarimFlinkInputSplit implements InputSplit {
    private final int splitNumber;
    private final TarimCombinedScanTask task;

    TarimFlinkInputSplit(int splitNumber, TarimCombinedScanTask task) {
        this.splitNumber = splitNumber;
        this.task = task;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

    CombinedScanTask getTask() {
        return task;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("splitNumber", splitNumber)
                .add("task", task)
                .toString();
    }
}
