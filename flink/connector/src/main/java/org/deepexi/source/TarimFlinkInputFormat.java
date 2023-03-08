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
import java.util.Iterator;
import java.util.List;

import org.deepexi.TarimDbAdapt;

public class TarimFlinkInputFormat  extends RichInputFormat<RowData, TarimFlinkInputSplit> {

    private final Table table;
    private final FileIO io;
    private final EncryptionManager encryption;
    private final TarimScanContext context;
    private final RowDataFileScanTaskReader rowDataReader;

    private transient DataIterator<RowData> iterator;
    private transient long currentReadCount = 0L;
    private final TarimDbAdapt tarimDbAdapt;
    private List<DeltaData> deltaData;
    private Iterator deltaDataIterator;
    private boolean endFlag = false;
    private DeltaData nextDeltaData;

    private int primaryIdInFlink;

    TarimFlinkInputFormat(TarimDbAdapt tarimDbAdapt, Table table, Schema tableSchema, FileIO io, EncryptionManager encryption,
                          TarimScanContext context) {
        this.tarimDbAdapt = tarimDbAdapt;
        this.table = table;
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
        return TarimFlinkSplitPlanner.planInputSplits(table, context);
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
        //todo get the deltaData from tarimDb
        try {
            deltaData = tarimDbAdapt.getDeltaData(((TarimCombinedScanTask)split.getTask()).getTrunk());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //todo, how to check if there is the pk?

        Iterator<Integer> iteratorTmp = context.project().identifierFieldIds().iterator();
        //only support 1 primary key
        if (iteratorTmp.hasNext()) {
            Object next =  iteratorTmp.next();
            //the column id in iceberg is larger 1 than the column id in flink
            primaryIdInFlink = ((int)next) - 1;
        }

        this.deltaDataIterator = deltaData.iterator();
        this.endFlag = false;
    }

    @Override
    public boolean reachedEnd() {
        if (context.limit() > 0 && currentReadCount >= context.limit()) {
            return true;
        } else {
            if (!iterator.hasNext()) {
                endFlag = true;

                //filter the data already merged
                while (deltaDataIterator.hasNext()) {
                    this.nextDeltaData = ((DeltaData) deltaDataIterator.next());
                    if (!nextDeltaData.getMatchFlag()) {
                        return false;
                    }
                }

                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public RowData nextRecord(RowData reuse) {
        currentReadCount++;

        if (!endFlag){
            RowData mainElement = iterator.next();
            RowData deltaElement;

            for (DeltaData delta: deltaData) {
                if (((GenericRowData) (delta.getData())).getField(primaryIdInFlink)
                        == ((GenericRowData) mainElement).getField(primaryIdInFlink)) {
                    deltaElement = delta.getData();
                    delta.setMatchFlag(true);
                    return deltaElement;
                }
            }

            return mainElement;
        }else{
            return nextDeltaData.getData();
        }
        //return iterator.next();
    }

    @Override
    public void close() throws IOException {
        if (iterator != null) {
            iterator.close();
        }
    }
}
