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

    boolean mainDataEnd;
    boolean deltaDataEnd;


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
        mergeState = MergeState.valueOf(0);

    }
    @Override
    public boolean reachedEnd() {
        mainDataEnd = !iterator.hasNext();
        deltaDataEnd = !deltaDataIterator.hasNext();

        if (context.limit() > 0 && currentReadCount >= context.limit()) {
            return true;
        } else {
            return mainDataEnd && deltaDataEnd;
        }
    }

    RowData getFirstRecord(DataIterator<RowData> iterator, DeltaDataIterator deltaDataIterator){
        if (equalState(STATE_LEFT_END)){
            return getNextRecordInStateLeftEnd(deltaDataIterator);
        }else if (equalState(STATE_RIGHT_END)){
            return getNextRecordInStateRightEnd(iterator);
        }

        //have to copy the mainRowData, because the parquet iterator will do getNext in hasNext function
        GenericRowData currentRowData = (GenericRowData)iterator.next();
        RowData rowData = copyMainRowData(currentRowData);

        deltaMinRowData = deltaDataIterator.next();
        mainPkMin = ((GenericRowData) rowData).getField(primaryIdInFlink).toString();
        deltaPkMin = ((GenericRowData) (deltaMinRowData.getData())).getField(primaryIdInFlink).toString();

        int compare = mainPkMin.compareTo(deltaPkMin);

        if(compare < 0){
            setState(STATE_LEFT);
            return rowData;
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

        if (mainDataEnd){
            setState(STATE_LEFT_END);
            //return deltaMinRowData.getData();
            return getNextRecordInStateLeftEnd(deltaDataIterator);
        }
        if (deltaDataEnd){
            setState(STATE_RIGHT_END);
            //return mainMinRowData;
            return getNextRecordInStateRightEnd(iterator);
        }

        //have to copy the mainRowData, because the parquet iterator will do getNext in hasNext function
        GenericRowData currentRowData = (GenericRowData)iterator.next();
        RowData rowData = copyMainRowData(currentRowData);

        deltaMinRowData = deltaDataIterator.next();
        mainPkMin = ((GenericRowData) rowData).getField(primaryIdInFlink).toString();
        deltaPkMin = ((GenericRowData) (deltaMinRowData.getData())).getField(primaryIdInFlink).toString();

        int compare = mainPkMin.compareTo(deltaPkMin);
        if(compare < 0){
            setState(STATE_LEFT);
            return rowData;
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
        if (mainDataEnd){
            setState(STATE_LEFT_END);
            return deltaMinRowData.getData();
        }

        GenericRowData mainElement = (GenericRowData)iterator.next();
        mainPkMin = mainElement.getField(primaryIdInFlink).toString();

        int compareCurrent = mainPkMin.compareTo(deltaPkMin);
        if (compareCurrent < 0){
            return mainElement;
        }else if (compareCurrent > 0){
            setState(STATE_RIGHT);
            copyMainRowData(mainElement);
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

        if (deltaDataEnd){
            setState(STATE_RIGHT_END);
            return mainMinRowData;
        }

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
        if (deltaData.getOp() == 2){
            //delete data merge
            //is there this case? the data deleted in delta but not in main
            getNextRecordInStateLeftEnd(deltaDataIterator);
        }
        return deltaData.getData();
    }

    private RowData copyMainRowData(GenericRowData orgRowData){

        int len = orgRowData.getArity();
        Object values[] = new Object[len];
        for (int i=0; i< len;i++){
            values[i] = orgRowData.getField(i);
        }
        mainMinRowData = GenericRowData.of(values);
        return mainMinRowData;
    }
    @Override
    public RowData nextRecord(RowData reuse) {

        RowData rowData;
        currentReadCount++;

        if (currentReadCount == 1){
            return getFirstRecord(iterator, deltaDataIterator);
        }

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
        STATE_EQUAL(0),
        STATE_LEFT(1),
        STATE_RIGHT(2),
        STATE_LEFT_END(3),
        STATE_RIGHT_END(4);

        private int value;
        MergeState(int value){
            this.value = value;
        }

        public static MergeState valueOf(int value){
            switch(value){
                case 0:
                    return MergeState.STATE_EQUAL;
                case 1:
                    return MergeState.STATE_LEFT;
                case 2:
                    return MergeState.STATE_RIGHT;
                case 3:
                    return MergeState.STATE_LEFT_END;
                case 4:
                    return MergeState.STATE_RIGHT_END;
                default:
                    return null;
            }
        }
    }
}
