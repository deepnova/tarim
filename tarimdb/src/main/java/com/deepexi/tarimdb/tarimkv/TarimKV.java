package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;
import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimKVProto.DistributionInfo;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * TarimKV
 *   TarimKV data server
 *
 */
public class TarimKV extends AbstractKV {

    public final static Logger logger = LogManager.getLogger(TarimKV.class);

    private SlotManager slotManager;
    private KVLocalMetadata lMetadata;
    private TarimKVMetaClient metaClient;

    public TarimKV(){
    }

    @Override
    public int init(KVLocalMetadata lMetadata) {
        this.lMetadata = lMetadata;
        getKVMetadata();
        slotManager.init(lMetadata.slots);
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

    public void put(KVSchema.KVChunk chunk, KVSchema.KeyValue value) {
        // no-op
    }

}

