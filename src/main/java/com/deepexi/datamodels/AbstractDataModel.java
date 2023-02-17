package com.deepexi.datamodels;

import java.lang.Thread;

import com.deepexi.tarimkv.*;
import com.deepexi.util.Status;
import com.deepexi.util.TLog;

/**
 * AbstractDataModel
 *
 */
public abstract class AbstractDataModel extends Thread {

    private Thread thread;
    protected TarimKV kv;

    public Status init(TarimKV kv) {
        if(kv == null) 
            throw new NullPointerException("kv can't be null.");
        this.kv = kv;
        return Status.OK;
    }

    @Override
    public void start() {
        TLog.info("Starting thread: " +  getName());
        if (thread == null) {
           thread = new Thread (this, getName());
           thread.start();
        }
    }
    public void run() {
        // no-op
    }
}

