package org.deepexi.sink;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;

import org.deepexi.TarimDbAdapt;
import org.deepexi.WResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


class TarimFilesCommitter extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<WResult, Void>, BoundedOneInput {

    private final Table table;
    private static TarimDbAdapt dbAdapter;

    private transient int subTaskId;
    private transient int attemptId;
    private transient String flinkJobId;
    private transient long maxCommittedCheckpointId;

    private static final long serialVersionUID = 1L;
    private static final long INITIAL_CHECKPOINT_ID = -1L;
    private static final Logger LOG = LoggerFactory.getLogger(TarimFilesCommitter.class);
    private static final String FLINK_JOB_ID = "flink.job-id";

    private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
    static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";
    private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = new ListStateDescriptor<>(
            "iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
    private transient ListState<String> jobIdState;
    // All pending checkpoints states for this function.
    private static final ListStateDescriptor<SortedMap<Long, byte[]>> STATE_DESCRIPTOR = buildStateDescriptor();
    private transient ListState<SortedMap<Long, byte[]>> checkpointsState;

    private final NavigableMap<Long, byte[]> dataListPerCheckpoint = Maps.newTreeMap();

    private final List<WResult> resultsOfCurrentCkPt = Lists.newArrayList();
    TarimFilesCommitter(Table table) {
        this.table = table;
    }

    public void open() {
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.attemptId = getRuntimeContext().getAttemptNumber();

        this.dbAdapter = new TarimDbAdapt(subTaskId, attemptId, table);
    }

    @Override
    public void endInput() {
        //todo
        long checkpointId = Long.MAX_VALUE;

        for ( WResult wr :resultsOfCurrentCkPt){
            for (byte[] data : wr.getDataList()){
                dataListPerCheckpoint.put(checkpointId, data);
            }
        }

        resultsOfCurrentCkPt.clear();
        dbAdapter.endData();
        dbAdapter.doCheckponit(checkpointId);

    }

    @Override
    public void processElement(StreamRecord<WResult> streamRecord) {
        //todo , do nothing?? needn't cache data for tarimDB here
        this.resultsOfCurrentCkPt.add(streamRecord.getValue());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        //todo
        super.snapshotState(context);
        long checkpointId = context.getCheckpointId();

        for ( WResult wr :resultsOfCurrentCkPt){
            for (byte[] data : wr.getDataList()){
                dataListPerCheckpoint.put(checkpointId, data);
            }
        }

        checkpointsState.clear();
        checkpointsState.add(dataListPerCheckpoint);

        jobIdState.clear();
        jobIdState.add(flinkJobId);

        resultsOfCurrentCkPt.clear();
    }
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
        this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

        this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
        this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);

        if (context.isRestored()) {
            String restoredFlinkJobId = jobIdState.get().iterator().next();
            Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
                    "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

            // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new flink job even if
            // it's restored from a snapshot created by another different flink job, so it's safe to assign the max committed
            // checkpoint id from restored flink job to the current flink job.
            this.maxCommittedCheckpointId = getMaxCommittedCheckpointId(table, restoredFlinkJobId);

            NavigableMap<Long, byte[]> uncommittedDataFiles = Maps
                    .newTreeMap(checkpointsState.get().iterator().next())
                    .tailMap(maxCommittedCheckpointId, false);
            if (!uncommittedDataFiles.isEmpty()) {
                // Committed all uncommitted data files from the old flink job to iceberg table.
                long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
                dbAdapter.doCheckponit(maxUncommittedCheckpointId);
                //commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId, maxUncommittedCheckpointId);
            }
        }

    }

    @Override
    public void notifyCheckpointComplete ( long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // It's possible that we have the following events:
        //   1. snapshotState(ckpId);
        //   2. snapshotState(ckpId+1);
        //   3. notifyCheckpointComplete(ckpId+1);
        //   4. notifyCheckpointComplete(ckpId);
        // For step#4, we don't need to commit iceberg table again because in step#3 we've committed all the files,
        // Besides, we need to maintain the max-committed-checkpoint-id to be increasing.
        if (checkpointId > maxCommittedCheckpointId) {
            dbAdapter.doCheckponit(checkpointId);
            this.maxCommittedCheckpointId = checkpointId;
        }
    }

    private static ListStateDescriptor<SortedMap<Long, byte[]>> buildStateDescriptor () {
        Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
        // Construct a SortedMapTypeInfo.
        SortedMapTypeInfo<Long, byte[]> sortedMapTypeInfo = new SortedMapTypeInfo<>(
                BasicTypeInfo.LONG_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, longComparator
        );
        return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
    }
    static long getMaxCommittedCheckpointId(Table table, String flinkJobId) {
        Snapshot snapshot = table.currentSnapshot();
        long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

        //todo, get the lastCommittedCheckpointId from TarimDB,or connector get it directly

        lastCommittedCheckpointId = dbAdapter.getLastommittedCheckpointId();

        return lastCommittedCheckpointId;
    }
}
