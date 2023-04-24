package org.deepexi.source;

import org.deepexi.TarimDbAdapt;

import java.util.Iterator;
import java.util.List;

public class DeltaDataIterator implements Iterator<DeltaData> {

    private List<DeltaData> deltaDatas = null;
    private int pos = 0;
    private boolean dataEnd = false;//must get data 1 time
    private TarimCombinedScanTask task;
    private TarimDbAdapt tarimDbAdapt;

    public DeltaDataIterator(TarimCombinedScanTask task, TarimDbAdapt tarimDbAdapt){
        this.task = task;
        this.tarimDbAdapt = tarimDbAdapt;
    }
    @Override
    public boolean hasNext() {

        if (deltaDatas == null){
            if (dataEnd){
                //after the first msg, there is not any data
                return false;
            }
            //the first msg
            TarimDbAdapt.DeltaRecords data = null;
            try {
                data = tarimDbAdapt.getDeltaData(
                        task.getTableID(),
                        task.getType(),
                        task.getPartitionID(),
                        task.getSchemaJson(),
                        task.getScanHandle(),
                        task.getHost(),
                        task.getPort(),
                        task.getPlanID(),
                        10,
                        task.getLowerBound(),
                        task.getUpperBound(),
                        task.getLowerBoundType(),
                        task.getUpperBoundType());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (data.datas == null) {
                return false;
            }

            if (data.datas.size() == 0){
                dataEnd = true;
                return false;
            }

            pos = 0;
            deltaDatas = data.datas;
            dataEnd = data.endData;
        }

        if(dataEnd && pos == deltaDatas.size()){
            return false;
        }
        return true;
    }

    @Override
    public DeltaData next() {
        DeltaData delta;
        TarimDbAdapt.DeltaRecords data;

        if (pos == deltaDatas.size()){
            //fetch the data
            try {
                if (!dataEnd){
                    data = tarimDbAdapt.getDeltaData(
                            task.getTableID(),
                            task.getType(),
                            task.getPartitionID(),
                            task.getSchemaJson(),
                            task.getScanHandle(),
                            task.getHost(),
                            task.getPort(),
                            task.getPlanID(),
                            10,
                            task.getLowerBound(),
                            task.getUpperBound(),
                            task.getLowerBoundType(),
                            task.getUpperBoundType());

                    if (data.datas == null) {
                        return null;
                    }

                    if (data.datas.size() == 0){
                        dataEnd = data.endData;
                        return null;
                    }

                    pos = 0;
                    deltaDatas = data.datas;
                    dataEnd = data.endData;
                }else{
                    return null;
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        delta = deltaDatas.get(pos);
        pos++;
        return delta;
    }
}
