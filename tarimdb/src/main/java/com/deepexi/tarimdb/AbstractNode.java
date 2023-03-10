package com.deepexi.tarimdb;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.util.BasicConfig;

/**
 * AbstractNode
 *
 */
public abstract class AbstractNode {
        
    protected BasicConfig conf_;

    public AbstractNode(BasicConfig conf){
        this.conf_ = conf;
    }
    public Status init() throws Exception { 
        return Status.OK;
    }
    public Status start(){
        return Status.OK;
    }
        
}


