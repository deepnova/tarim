package com.deepexi.tarimdb.datamodels;

import java.lang.Thread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.tarimkv.*;
import com.deepexi.tarimdb.util.Status;

/**
 * AbstractDataModel
 *
 */
public abstract class AbstractDataModel extends Thread {

    public final static Logger logger = LogManager.getLogger(AbstractDataModel.class);

    //private Thread thread;
    protected TarimKVClient kv;

    public Status init(TarimKVClient kv) {
        if(kv == null) 
            throw new NullPointerException("kv can't be null.");
        this.kv = kv;
        return Status.OK;
    }

    public TarimKVClient getTarimKVClient(){
        return this.kv;
    }
    @Override
    public void start() {
        /*logger.info("Starting thread: " +  getName());
        if (thread == null) {
           thread = new Thread (this, getName());
           thread.start();
        }*/
    }
    public void run() {
        // no-op
    }
}

