package com.deepexi;

import com.deepexi.util.TLog;
import com.deepexi.util.BasicConfig;
import com.deepexi.util.Status;
import com.deepexi.tarimkv.TarimKVMetaClient;
import com.deepexi.rpc.TarimKVMetaSvc.DistributionInfo;

/**
 * DataNode
 *
 */
public class DataNode extends AbstractNode {
    public DataNode(BasicConfig conf){
        super(conf);
    }

    @Override
    public Status init(){ 
        TLog.info("datanode init");
        return Status.OK;
    }

    @Override
    public Status start(){ 
        TLog.info("datanode run");

        try{
            //testKVMetaClient();
            getKVMetadata();
        } catch(InterruptedException e){
            TLog.error("InterruptedException error");
        }
        return Status.OK;
    }

    public void getKVMetadata() throws InterruptedException{

        TarimKVMetaClient client = new TarimKVMetaClient("127.0.0.1",1301);
        DistributionInfo dist = client.getDistribution();
    }

    public void testKVMetaClient() throws InterruptedException{

        TarimKVMetaClient client = new TarimKVMetaClient("127.0.0.1",50051);
        for(int i=0; i<5; i++){
            client.sayHello("client word:"+ i);
            Thread.sleep(3000);
        }
    }
}

