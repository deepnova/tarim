package com.deepexi;

import com.deepexi.util.Status;
import com.deepexi.util.TLog;
import com.deepexi.util.BasicConfig;

/**
 * AbstractNode
 *
 */
public abstract class AbstractNode {
        
    protected BasicConfig conf_;

    public AbstractNode(BasicConfig conf){
        this.conf_ = conf;
    }
    public Status init(){ 
        return Status.OK;
    }
    public Status start(){
        return Status.OK;
    }
        
}


