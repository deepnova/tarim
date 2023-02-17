package com.deepexi;

import java.lang.NullPointerException;
import java.util.ArrayList;

import com.deepexi.util.TLog;
import com.deepexi.util.BasicConfig;
import com.deepexi.util.Status;
import com.deepexi.tarimkv.TarimKVMetaClient;
import com.deepexi.tarimkv.TarimKV;
import com.deepexi.tarimkv.KVLocalMetadata;
import com.deepexi.tarimkv.YamlLoader;
//import com.deepexi.rpc.TarimKVMetaSvc;
//import com.deepexi.rpc.TarimKVMetaSvc.DistributionInfo;

import com.deepexi.datamodels.*;

/**
 * DataNode
 *
 */
public class DataNode extends AbstractNode {

    private TarimKV kv;
    private KVLocalMetadata metadata;
    private ArrayList<AbstractDataModel> models;

    public DataNode(BasicConfig conf){
        super(conf);
        kv = new TarimKV();
        metadata = new KVLocalMetadata();
        models = new ArrayList();
    }

    private void loadDataModels() {
        TarimDB db = new TarimDB();
        db.init(kv);
        models.add(db);
    }

    @Override
    public Status init(){ 
        TLog.info("datanode init");
        YamlLoader.loadDNodeConfig(conf_.configFile, metadata);
        kv.init(metadata);
        loadDataModels();
        return Status.OK;
    }

    @Override
    public Status start(){ 
        TLog.info("datanode run");

        try{
            for(AbstractDataModel model : models) {
                model.start();
            }
            for(AbstractDataModel model : models) {
                model.join();
            }
        } catch(Exception e){
            TLog.error("Exception message: " + e.getMessage());
        }
        return Status.OK;
    }
}

