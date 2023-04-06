package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import com.deepexi.TarimMetaClient;
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

    private TarimMetaClient metaClient;
    //private KVMetadata metadata;
    private KVLocalMetadata lMetadata;
    private TarimKVLocal kvLocal;

    public TarimKVClient(KVLocalMetadata lMetadata){
        this.lMetadata = lMetadata;
    }

    // for unit test
    public TarimKVClient(TarimMetaClient metaClient, KVLocalMetadata lMetadata){
        this.metaClient = metaClient;
        this.lMetadata = lMetadata;
    }

    public KVLocalMetadata getKVLocalMetadata(){
        return this.lMetadata;
    }

    public int init() throws Exception
    {
        getKVMetadata();
        kvLocal = new TarimKVLocal(metaClient, lMetadata);
        try{
            kvLocal.init();
        } catch (RocksDBException e){
            logger.error("client init, rocksdb exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
        return 0;
    }

    private void getKVMetadata() {
        if(metaClient == null){
            TarimKVProto.Node node = lMetadata.getMasterMNode();
            if(node == null) throw new NullPointerException("Not found master meta node"); 
            logger.info("get kv metadata from host: " + node.getHost() + ", port: " + node.getPort());
            metaClient = new TarimMetaClient(node.getHost(), node.getPort());
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
    
    public List<TarimKVProto.KeyValueByte> prefixSeek(PrefixSeekRequest request) throws TarimKVException
    {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        try {
            return kvLocal.prefixSeek(request);
        }
        catch (RocksDBException e)
        {
            logger.error("rocksdb exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
    }

    public KVSchema.PrepareScanInfo prepareChunkScan(int tableID, long[] chunks) throws TarimKVException
    {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        try {
            return kvLocal.prepareChunkScan(tableID, chunks);
        }
        catch (RocksDBException e)
        {
            logger.error("rocksdb exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
    }

    public List<TarimKVProto.KeyValueOp> deltaChunkScan(KVSchema.DeltaScanParam param, boolean ifComplete)
              throws TarimKVException
    {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        try {
            return kvLocal.deltaChunkScan(param, ifComplete);
        }
        catch (RocksDBException e)
        {
            logger.error("rocksdb exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new TarimKVException(Status.ROCKSDB_ERROR);
        }
    }

    public void closeChunkScan(int tableID, KVSchema.ChunkScanHandler[] scanHandlers) throws TarimKVException
    {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        kvLocal.closeChunkScan(tableID, scanHandlers);
    }

    public void closeChunkScan(int tableID, long chunkID, long scanHandler) throws TarimKVException
    {
        //TODO: write local or remote rocksdb here if necessary
        //TODO: check if distribution is correct. 
        kvLocal.closeChunkScan(tableID, chunkID, scanHandler);
    }
}

