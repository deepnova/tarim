package org.deepexi.source;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.FileIO;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.deepexi.TarimDbAdapt;

import static org.deepexi.source.TarimFlinkInputFormat.MergeState.*;

public class TarimFlinkInputFormat  extends RichInputFormat<RowData, TarimFlinkInputSplit> {

    private final Table tarimTable;
    private final Table icebergTable;
    private final FileIO io;
    private final EncryptionManager encryption;
    private final TarimScanContext context;
    private final RowDataFileScanTaskReader rowDataReader;

    private transient DataIterator<RowData> iterator;
    private transient long currentReadCount = 0L;
    private final TarimDbAdapt tarimDbAdapt;
    private DeltaDataIterator deltaDataIterator;

    private int primaryIdInFlink;

    private MergeState mergeState;
    private String mainPkMin;
    private String deltaPkMin;

    private DeltaData deltaMinRowData;
    private RowData mainMinRowData;


    TarimFlinkInputFormat(TarimDbAdapt tarimDbAdapt, Table tarimTable, Table icebergTable, Schema tableSchema, FileIO io, EncryptionManager encryption,
                          TarimScanContext context) {
        this.tarimDbAdapt = tarimDbAdapt;
        this.tarimTable = tarimTable;
        this.icebergTable = icebergTable;
        this.io = io;
        this.encryption = encryption;
        this.context = context;
        this.rowDataReader = new RowDataFileScanTaskReader(tableSchema,
                context.project(), context.nameMapping(), context.caseSensitive());

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        // Legacy method, not be used.
        return null;
    }

    @Override
    public TarimFlinkInputSplit[] createInputSplits(int minNumSplits) {
        // Called in Job manager, so it is OK to load table from catalog.
        return TarimFlinkSplitPlanner.planInputSplits(tarimTable, icebergTable, context);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(TarimFlinkInputSplit[] tarimFlinkInputSplits) {
        return new DefaultInputSplitAssigner(tarimFlinkInputSplits);
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(TarimFlinkInputSplit split) {

        List<Integer> identifierIdsList;
        this.iterator = new DataIterator<>(rowDataReader, split.getTask(), io, encryption);
        this.deltaDataIterator = new DeltaDataIterator((TarimCombinedScanTask)split.getTask(), tarimDbAdapt);

        //only support 1 primary key

        identifierIdsList = new ArrayList<>(context.project().identifierFieldIds());
        if (identifierIdsList.size() < 1){
            throw new RuntimeException("open TarimFlinkInputFormat error! identifierFieldIds is null!");
        }
        //the column id in iceberg is larger 1 than the column id in flink
        //this one primary key
        primaryIdInFlink = identifierIdsList.get(0) - 1;

    }
    @Override
    public boolean reachedEnd() {
        boolean mainDataEnd = iterator.hasNext();
        boolean deltaDataEnd = deltaDataIterator.hasNext();

        if (mainDataEnd){
            setState(STATE_LEFT_END);
        }
        if (deltaDataEnd){
            setState(STATE_RIGHT_END);
        }

        if (context.limit() > 0 && currentReadCount >= context.limit()) {
            return true;
        } else {
            return !mainDataEnd && !deltaDataEnd;
        }
    }

    RowData getFirstRecord(DataIterator<RowData> iterator, DeltaDataIterator deltaDataIterator){
        if (equalState(STATE_LEFT_END)){
            return getNextRecordInStateLeftEnd(deltaDataIterator);
        }else if (equalState(STATE_RIGHT_END)){
            return getNextRecordInStateRightEnd(iterator);
        }

        mainMinRowData = iterator.next();
        deltaMinRowData = deltaDataIterator.next();
        mainPkMin = ((GenericRowData) mainMinRowData).getField(primaryIdInFlink).toString();
        deltaPkMin = ((GenericRowData) (deltaMinRowData.getData())).getField(primaryIdInFlink).toString();

        int compare = mainPkMin.compareTo(deltaPkMin);

        if(compare < 0){
            setState(STATE_LEFT);
            return mainMinRowData;
        }else if(compare > 0){
            setState(STATE_RIGHT);
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                //is there this case? the data deleted in delta but not in main
                getNextRecordInStateLeft(iterator);
            }
            return deltaMinRowData.getData();
        }else{
            setState(STATE_EQUAL);
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                getNextRecordInStateEqual(iterator, deltaDataIterator);
            }
            return deltaMinRowData.getData();
        }
    }

    RowData getNextRecordInStateEqual(DataIterator<RowData> iterator, DeltaDataIterator deltaDataIterator){

        mainMinRowData = iterator.next();
        deltaMinRowData = deltaDataIterator.next();
        mainPkMin = ((GenericRowData) mainMinRowData).getField(primaryIdInFlink).toString();
        deltaPkMin = ((GenericRowData) (deltaMinRowData.getData())).getField(primaryIdInFlink).toString();

        int compare = mainPkMin.compareTo(deltaPkMin);
        if(compare < 0){
            setState(STATE_LEFT);
            return mainMinRowData;
        }else if(compare > 0){
            setState(STATE_RIGHT);
            return deltaMinRowData.getData();
        }else{
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                getNextRecordInStateEqual(iterator, deltaDataIterator);
            }
            return deltaMinRowData.getData();
        }
    }

    RowData getNextRecordInStateLeft(DataIterator<RowData> iterator){
        RowData mainElement = iterator.next();
        mainPkMin = ((GenericRowData) mainElement).getField(primaryIdInFlink).toString();

        int compareCurrent = mainPkMin.compareTo(deltaPkMin);
        if (compareCurrent < 0){
            return mainElement;
        }else if (compareCurrent > 0){
            setState(STATE_RIGHT);
            mainMinRowData = mainElement;
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                //is there this case? the data deleted in delta but not in main
                getNextRecordInStateLeft(iterator);
            }
            return deltaMinRowData.getData();
        }else{
            setState(STATE_EQUAL);
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                getNextRecordInStateEqual(iterator, deltaDataIterator);
            }
            return deltaMinRowData.getData();
        }
    }

    RowData getNextRecordInStateRight(DeltaDataIterator deltaDataIterator){
        DeltaData deltaElement = deltaDataIterator.next();

        deltaPkMin = ((GenericRowData) deltaElement.getData()).getField(primaryIdInFlink).toString();

        int compare = mainPkMin.compareTo(deltaPkMin);
        if(compare < 0){
            setState(STATE_LEFT);
            deltaMinRowData = deltaElement;
            return mainMinRowData;
        }else if(compare > 0){
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                //is there this case? the data deleted in delta but not in main
                getNextRecordInStateRight(deltaDataIterator);
            }
            return deltaElement.getData();
        }else{
            setState(STATE_EQUAL);
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                getNextRecordInStateEqual(iterator, deltaDataIterator);
            }
            return deltaElement.getData();
        }
    }

    RowData getNextRecordInStateRightEnd(DataIterator<RowData> iterator){
        return iterator.next();
    }

    RowData getNextRecordInStateLeftEnd(DeltaDataIterator deltaDataIterator){
        DeltaData deltaData = deltaDataIterator.next();
        if (deltaMinRowData.getOp() == 2){
            //delete data merge
            //is there this case? the data deleted in delta but not in main
            getNextRecordInStateLeftEnd(deltaDataIterator);
        }
        return deltaData.getData();
    }

    @Override
    public RowData nextRecord(RowData reuse) {

        RowData rowData;
        if (currentReadCount == 0){
            return getFirstRecord(iterator, deltaDataIterator);
        }

        currentReadCount++;

        switch (mergeState) {
            case STATE_EQUAL:
                rowData = getNextRecordInStateEqual(iterator, deltaDataIterator);
                break;
            case STATE_LEFT:
                rowData = getNextRecordInStateLeft(iterator);
                break;
            case STATE_RIGHT:
                rowData = getNextRecordInStateRight(deltaDataIterator);
                break;
            case STATE_LEFT_END:
                rowData = getNextRecordInStateLeftEnd(deltaDataIterator);
                break;
            case STATE_RIGHT_END:
                rowData = getNextRecordInStateRightEnd(iterator);
                break;
            default:
                throw new RuntimeException("nextRecord error!");
        }
        return rowData;
    }

    @Override
    public void close() throws IOException {
        if (iterator != null) {
            iterator.close();
        }
    }

    private void setState(MergeState state){
        mergeState = state;
    }

    private boolean equalState(MergeState state){
        return mergeState.equals(state);
    }
    enum MergeState {
        STATE_EQUAL,
        STATE_LEFT,
        STATE_RIGHT,
        STATE_LEFT_END,
        STATE_RIGHT_END
    }
}
