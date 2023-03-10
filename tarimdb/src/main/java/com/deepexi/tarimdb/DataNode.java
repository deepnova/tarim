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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

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

    private int port = 1302;
    private Server server;

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
    public Status init() throws Exception { 
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
            ServerBuilder builder = ServerBuilder.forPort(port);
            builder.addService(new TarimDBServer());
            server = builder.build().start();
            logger.info("TarimDB service start...");

            //添加停机逻辑
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    logger.info("*** shutting down data node server since JVM is shutting down");
                    DataNode.this.stop();
                    logger.info("*** Tarim server shut down");
                }
            });

        } catch(IOException e){
            logger.error("DataNode start error(IOException)");
            return Status.SERVER_START_FAILED;
        }

        return Status.OK;
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}

