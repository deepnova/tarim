package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;
import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimKVProto.*;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * TarimKVClient
 *   for access business metadata
 */
public class TarimKVClient {

    public final static Logger logger = LogManager.getLogger(TarimKVClient.class);

    private TarimKVMetaClient metaClient;
    //private KVMetadata metadata;
    private KVLocalMetadata lMetadata;

    public TarimKVClient(KVLocalMetadata lMetadata){
        this.lMetadata = lMetadata;
    }

    public int init() {
        getKVMetadata();
        return 0;
    }

    private void getKVMetadata() {
        if(metaClient == null){
            TarimKVProto.Node node = lMetadata.getMasterMNode();
            if(node == null) throw new NullPointerException("Not found master meta node"); 
            logger.info("get kv metadata from host: " + node.getHost() + ", port: " + node.getPort());
            metaClient = new TarimKVMetaClient(node.getHost(), node.getPort());
        }
        metaClient.getDistribution();
    }

    public void put(PutRequest request) {
        // no-op
    }
}

