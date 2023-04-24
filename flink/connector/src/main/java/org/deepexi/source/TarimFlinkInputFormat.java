package org.deepexi.source;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.FileIO;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.types.Type;
import org.deepexi.ConnectorTarimTable;
import org.deepexi.TarimDbAdapt;
import org.deepexi.TarimPrimaryKey;

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
    private MergeSubState mergeSubState;
    private Object[] mainPkMin;
    private Object[] deltaPkMin;

    private DeltaData deltaMinRowData;
    private RowData mainMinRowData;

    boolean mainDataEnd;
    boolean deltaDataEnd;
    List<Integer> identifierIdsList;
    TarimPrimaryKey tarimPrimaryKey;
    private transient RowDataWrapper wrapper;

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
        this.tarimPrimaryKey = ((ConnectorTarimTable)tarimTable).getPrimaryKey();
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

        this.iterator = new DataIterator<>(rowDataReader, split.getTask(), io, encryption);
        this.deltaDataIterator = new DeltaDataIterator((TarimCombinedScanTask)split.getTask(), tarimDbAdapt);

        this.identifierIdsList = new ArrayList<>(context.project().identifierFieldIds());
        if (identifierIdsList.size() < 1){
            throw new RuntimeException("open TarimFlinkInputFormat error! identifierFieldIds is null!");
        }

        mainPkMin = new Object[identifierIdsList.size()];
        deltaPkMin = new Object[identifierIdsList.size()];

        this.wrapper = new RowDataWrapper(FlinkSchemaUtil.convert(context.project()), context.project().asStruct());

        this.currentReadCount = 0L;
        mergeState = MergeState.valueOf(0);
        mergeSubState = MergeSubState.valueOf(0);
    }

    @Override
    public boolean reachedEnd() {
        mainDataEnd = !iterator.hasNext();
        deltaDataEnd = !deltaDataIterator.hasNext();

        if (mainDataEnd){
            if (currentReadCount == 0) {
                setState(STATE_LEFT_END);
            }

            if (equalSubState(MergeSubState.SUB_STATE_LEFT_NOT_COLLECT)){
                setSubState(MergeSubState.SUB_STATE_LEFT_REVERSE_LAST);
            }
        }

        if (deltaDataEnd){
            if (currentReadCount == 0){
                setState(STATE_RIGHT_END);
            }

            if (equalSubState(MergeSubState.SUB_STATE_RIGHT_NOT_COLLECT)){
                setSubState(MergeSubState.SUB_STATE_RIGHT_REVERSE_LAST);
            }
        }

        if (context.limit() > 0 && currentReadCount >= context.limit()) {
            return true;
        }

        if (mainDataEnd && deltaDataEnd) {
            if (equalSubState(MergeSubState.SUB_STATE_RIGHT_REVERSE_LAST)) {
                setState(STATE_RIGHT_LAST_DATA);
                return false;
            }
            if (equalSubState(MergeSubState.SUB_STATE_LEFT_REVERSE_LAST)) {
                setState(STATE_LEFT_LAST_DATA);
                return false;
            }

            return true;
        }
        return false;


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

        mainPkMin = tarimPrimaryKey.primaryData(wrapper.wrap(rowData));
        deltaPkMin = tarimPrimaryKey.primaryData(wrapper.wrap(deltaMinRowData.getData()));

        int compare = comparePrimary(mainPkMin, deltaPkMin);
        if(compare < 0){
            setState(STATE_LEFT);
            setSubState(MergeSubState.SUB_STATE_RIGHT_NOT_COLLECT);
            return rowData;
        }else if(compare > 0){
            setState(STATE_RIGHT);
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                //is there this case? the data deleted in delta but not in main
                getNextRecordInStateLeft(iterator);
            }
            setSubState(MergeSubState.SUB_STATE_LEFT_NOT_COLLECT);
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

        mainPkMin = tarimPrimaryKey.primaryData(wrapper.wrap(rowData));
        deltaPkMin = tarimPrimaryKey.primaryData(wrapper.wrap(deltaMinRowData.getData()));

        int compare = comparePrimary(mainPkMin, deltaPkMin);
        if(compare < 0){
            setState(STATE_LEFT);
            setSubState(MergeSubState.SUB_STATE_RIGHT_NOT_COLLECT);
            return rowData;
        }else if(compare > 0){
            setState(STATE_RIGHT);
            setSubState(MergeSubState.SUB_STATE_LEFT_NOT_COLLECT);
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
            setSubState(MergeSubState.SUB_STATE_INIT);
            return deltaMinRowData.getData();
        }

        GenericRowData mainElement = (GenericRowData)iterator.next();
        mainPkMin = tarimPrimaryKey.primaryData(wrapper.wrap(mainElement));

        int compare = comparePrimary(mainPkMin, deltaPkMin);
        if (compare < 0){
            return mainElement;
        }else if (compare > 0){
            setState(STATE_RIGHT);
            copyMainRowData(mainElement);
            if (deltaMinRowData.getOp() == 2){
                //delete data merge
                //is there this case? the data deleted in delta but not in main
                getNextRecordInStateLeft(iterator);
            }
            setSubState(MergeSubState.SUB_STATE_LEFT_NOT_COLLECT);
            return deltaMinRowData.getData();
        }else{
            setState(STATE_EQUAL);
            if (deltaMinRowData.getOp() == 2){
                getNextRecordInStateEqual(iterator, deltaDataIterator);
            }

            setSubState(MergeSubState.SUB_STATE_INIT);
            return deltaMinRowData.getData();
        }
    }

    RowData getNextRecordInStateRight(DeltaDataIterator deltaDataIterator){

        if (deltaDataEnd){
            setState(STATE_RIGHT_END);
            setSubState(MergeSubState.SUB_STATE_INIT);
            return mainMinRowData;
        }

        DeltaData deltaElement = deltaDataIterator.next();

        deltaPkMin = tarimPrimaryKey.primaryData(wrapper.wrap(deltaElement.getData()));
        int compare = comparePrimary(mainPkMin, deltaPkMin);

        if(compare < 0){
            setState(STATE_LEFT);
            deltaMinRowData = deltaElement;

            setSubState(MergeSubState.SUB_STATE_RIGHT_NOT_COLLECT);
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
            setSubState(MergeSubState.SUB_STATE_INIT);
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
            case STATE_LEFT_LAST_DATA:
                rowData = mainMinRowData;
                setSubState(MergeSubState.SUB_STATE_INIT);
                break;
            case STATE_RIGHT_LAST_DATA:
                rowData = deltaMinRowData.getData();
                setSubState(MergeSubState.SUB_STATE_INIT);
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
        STATE_RIGHT_END(4),
        STATE_LEFT_LAST_DATA(5),
        STATE_RIGHT_LAST_DATA(6);

        private int value;

        MergeState(int value) {
            this.value = value;
        }

        public static MergeState valueOf(int value) {
            switch (value) {
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
                case 5:
                    return MergeState.STATE_LEFT_LAST_DATA;
                case 6:
                    return MergeState.STATE_RIGHT_LAST_DATA;
                default:
                    return null;
            }
        }
    }

    private boolean equalSubState(MergeSubState state){
        return mergeSubState.equals(state);
    }
    private void setSubState(MergeSubState state){
        mergeSubState = state;
    }

    enum MergeSubState {
        SUB_STATE_INIT(0),
        SUB_STATE_RIGHT_NOT_COLLECT(1),
        SUB_STATE_RIGHT_REVERSE_LAST(2),
        SUB_STATE_LEFT_NOT_COLLECT(3),
        SUB_STATE_LEFT_REVERSE_LAST(4);

        private int value;
        MergeSubState(int value) {
            this.value = value;
        }
        public static MergeSubState valueOf(int value) {
            switch (value) {
                case 0:
                    return MergeSubState.SUB_STATE_INIT;
                case 1:
                    return MergeSubState.SUB_STATE_RIGHT_NOT_COLLECT;
                case 2:
                    return MergeSubState.SUB_STATE_RIGHT_REVERSE_LAST;
                case 3:
                    return MergeSubState.SUB_STATE_LEFT_NOT_COLLECT;
                case 4:
                    return MergeSubState.SUB_STATE_LEFT_REVERSE_LAST;
                default:
                    return null;
            }
        }
    }


    private int comparePrimary(Object[] mainPkMin, Object[] deltaPkMin){

        for (Integer id : identifierIdsList){
            Type.TypeID type = context.project().findType(id).typeId();

            int newID = id - 1;
            switch(type){
                case INTEGER:
                    Integer mainKey = (Integer)mainPkMin[newID];
                    Integer deltaKey = (Integer)deltaPkMin[newID];
                    if (mainKey > deltaKey){
                        return 1;
                    }else if (mainKey < deltaKey){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                case LONG:
                    Long mainLongKey = (Long)mainPkMin[newID];
                    Long deltaLongKey = (Long)deltaPkMin[newID];
                    if (mainLongKey > deltaLongKey){
                        return 1;
                    }else if (mainLongKey < deltaLongKey){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                case STRING:
                    String mainStrKey = (String)mainPkMin[newID];
                    String deltaStrKey = (String)deltaPkMin[newID];

                    int result = mainStrKey.compareTo(deltaStrKey);
                    if (result > 0){
                        return 1;
                    }else if (result < 0){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                case DATE:
                case TIME:
                case TIMESTAMP:
                default:
                    throw new RuntimeException("un support Type !!");
            }
        }

        return 0;
    }
}
