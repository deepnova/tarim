package com.deepexi.tarimdb;

import java.lang.NullPointerException;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;
//import com.deepexi.tarimdb.tarimkv.TarimKVMetaClient;
import com.deepexi.tarimdb.tarimkv.TarimKVClient;
import com.deepexi.tarimdb.tarimkv.KVLocalMetadata;
import com.deepexi.tarimdb.tarimkv.YamlLoader;

import com.deepexi.tarimdb.datamodels.*;

/**
 * DataNode
 *
 */
public class DataNode extends AbstractNode {

    public final static Logger logger = LogManager.getLogger(DataNode.class);
    private TarimKVClient kvClient;
    private KVLocalMetadata lMetadata;
    private ArrayList<AbstractDataModel> models;

    public DataNode(BasicConfig conf){
        super(conf);
        lMetadata = new KVLocalMetadata();
        kvClient = new TarimKVClient(lMetadata);
        models = new ArrayList();
    }

    private void loadDataModels() {
        TarimDB db = new TarimDB();
        db.init(kvClient);
        models.add(db);
    }

    @Override
    public Status init(){ 
        logger.info("datanode init");
        YamlLoader.loadDNodeConfig(conf_.configFile, lMetadata);
        kvClient.init();
        loadDataModels();
        return Status.OK;
    }

    @Override
    public Status start(){ 
        logger.info("datanode run");

        try{
            for(AbstractDataModel model : models) {
                model.start(); //TODO: thread pool
            }
            for(AbstractDataModel model : models) {
                model.join();
            }
        } catch(Exception e){
            logger.error("Exception message: " + e.getMessage());
        }
        return Status.OK;
    }
}

