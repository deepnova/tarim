package com.deepexi.datamodels;

import com.deepexi.util.TLog;
import com.deepexi.util.Status;
import com.deepexi.tarimkv.*;
/**
 * TarimDB
 *
 */
public class TarimDB extends AbstractDataModel {

    public TarimDB() {
    }

    @Override
    public Status init(TarimKV kv) {
        super.init(kv);
        TLog.info( "This is TarimDB!" );
        return Status.OK;
    }

    @Override
    public void run() {
        //TODO
        TLog.info( "Do some work in TarimDB!" );
    }
}
