package org.deepexi.source;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

public class TarimCombinedScanTask implements CombinedScanTask {

    private String trunkInfo;
    private final List<FileScanTask> tasks;

    public TarimCombinedScanTask( List<FileScanTask>  tasks) {
        Preconditions.checkNotNull(tasks, "tasks cannot be null");
        this.tasks = tasks;
    }
    public TarimCombinedScanTask(String trunkInfo, List<FileScanTask>  tasks) {
        this.trunkInfo = trunkInfo;
        this.tasks = tasks;
    }
    @Override
    public Collection<FileScanTask> files() {
        return ImmutableList.copyOf(this.tasks);
    }

    public void addTask(FileScanTask task){
        tasks.add(task);
    }

    public void setTrunk(String trunkInfo){
        this.trunkInfo = trunkInfo;
    }

    public String getTrunk(){
        return this.trunkInfo;
    }
}
