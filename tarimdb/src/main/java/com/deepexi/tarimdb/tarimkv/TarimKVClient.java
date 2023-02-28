package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.TarimKVException;
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
    private TarimKVLocal kvLocal;

    public TarimKVClient(KVLocalMetadata lMetadata){
        this.lMetadata = lMetadata;
    }

    // for unit test
    public TarimKVClient(TarimKVMetaClient metaClient, KVLocalMetadata lMetadata){
        this.metaClient = metaClient;
        this.lMetadata = lMetadata;
    }

    public int init() {
        getKVMetadata();
        kvLocal = new TarimKVLocal(metaClient, lMetadata);
        kvLocal.init();
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

    public void put(PutRequest request) throws TarimKVException {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        try{
            kvLocal.put(request);
        } catch (RocksDBException e){
            logger.error("rocksdb exception: %s\n", e.getMessage());
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
    }
    public List<byte[]> get(GetRequest request) throws TarimKVException {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        try{
            return kvLocal.get(request);
        } catch (RocksDBException e){
            logger.error("rocksdb exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
    }

    public void delete(DeleteRequest request) throws TarimKVException 
    {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        try{
            kvLocal.delete(request);
        } catch (RocksDBException e){
            logger.error("rocksdb exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
    }
    
    public List<KeyValue> prefixSeek(PrefixSeekRequest request) throws TarimKVException {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        try{
            return kvLocal.prefixSeek(request);
        } catch (RocksDBException e){
            logger.error("rocksdb exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
    }
}

