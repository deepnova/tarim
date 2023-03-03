package com.deepexi.tarimdb.datamodels;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.tarimkv.*;
/**
 * TarimDB
 *
 */
public class TarimDB extends AbstractDataModel {

    public final static Logger logger = LogManager.getLogger(TarimDB.class);

    public TarimDB() {
    }

    @Override
    public Status init(TarimKVClient kv) {
        super.init(kv);
        logger.info( "This is TarimDB!" );
        return Status.OK;
    }

    @Override
    public void run() {
        //TODO
        logger.info( "Do some work in TarimDB!" );
    }
}
