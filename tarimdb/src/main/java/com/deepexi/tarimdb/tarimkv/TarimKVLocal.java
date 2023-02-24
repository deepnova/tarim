package com.deepexi.tarimdb.tarimkv;

import java.util.List;
import java.util.Set;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVProto.*;
import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimKVMetaGrpc;
import com.deepexi.rpc.TarimKVProto.DataDistributionRequest;
import com.deepexi.rpc.TarimKVProto.DataDistributionResponse;
import com.deepexi.rpc.TarimKVProto.DistributionInfo;
import com.deepexi.tarimdb.util.TarimKVException;
import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.util.Common;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * TarimKVLocal
 *  
 *
 */
public class TarimKVLocal {

    public final static Logger logger = LogManager.getLogger(TarimKVLocal.class);

    private KVLocalMetadata lMetadata;
    private SlotManager slotManager;
    private TarimKVMetaClient metaClient;

    public TarimKVLocal(TarimKVMetaClient metaClient, KVLocalMetadata lMetadata) {
        this.metaClient = metaClient;
        this.lMetadata = lMetadata;
        slotManager = new SlotManager();
        logger.debug("TarimKVLocal constructor, local metadata: " + lMetadata.toString());
    }

    public void init(){
        slotManager.init(lMetadata.slots);
    }

    private Slot getSlot(int chunkID) throws TarimKVException {
        boolean refreshed = false;
        String slotID;
        do{
            slotID = metaClient.getMasterReplicaSlot(chunkID);
            KVLocalMetadata.Node node = metaClient.getReplicaNode(slotID);
            if(node.host != lMetadata.address || node.port != lMetadata.port){
                if(refreshed == true){
                    throw new TarimKVException(Status.DISTRIBUTION_ERROR);
                }
                metaClient.refreshDistribution();
                refreshed = true;
            }else{
                break;
            }
        }while(true);

        Slot slot = slotManager.getSlot(slotID);
        if(slot == null){
            throw new TarimKVException(Status.MASTER_SLOT_NOT_FOUND);
        }
        return slot;
    } 

    private void validPutParam(PutRequest request) throws TarimKVException{
        if(request.getTableID() > 0
           && request.getChunkID() > 0 
           && request.getValuesCount() > 0) {
           return;
        }
        throw new TarimKVException(Status.PARAM_ERROR);
    }
    /**
     */
    public void put(PutRequest request) throws RocksDBException, TarimKVException {
        validPutParam(request);
        String cfName = Integer.toString(request.getTableID());

        Slot slot = getSlot(request.getChunkID());
        slot.createColumnFamilyIfNotExist(cfName);
        ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);

        // writing key-values
        WriteOptions writeOpt = new WriteOptions();
        WriteBatch batch = new WriteBatch();
        String key;
        for(TarimKVProto.KeyValue kv : request.getValuesList()){
            key = KeyValueCodec.KeyEncode( new KeyValueCodec(request.getChunkID(), kv) );
            batch.put(cfh
                      ,key.getBytes()
                      ,kv.getValue().getBytes());
            logger.debug("for WriteBatch, key: " + key + ", value: " + kv.getValue());
        }
        slot.batchWrite(writeOpt, batch);
    }

    private void validGetParam(GetRequest request) throws TarimKVException
    {
        if(request.getTableID() > 0
           && request.getChunkID() > 0 
           && request.getKeysCount() > 0) 
        {
           return;
        }
        throw new TarimKVException(Status.PARAM_ERROR);
    }
    /**
     */
    public List<byte[]> get(GetRequest request) throws RocksDBException, TarimKVException
    {
        validGetParam(request);
        String cfName = Integer.toString(request.getTableID());
        Slot slot = getSlot(request.getChunkID());
        ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);

        ReadOptions readOpt = new ReadOptions();
        List<byte[]> values = slot.multiGet(readOpt, cfh, request.getKeysList());

        return values;
    }

    private void validDeleteParam(DeleteRequest request) throws TarimKVException
    {
        if(request.getTableID() > 0
           && request.getChunkID() > 0 
           && request.getKey() != null && !request.getKey().isEmpty()) 
        {
           return;
        }
        throw new TarimKVException(Status.PARAM_ERROR);
    }
    /**
     */
    public void delete(DeleteRequest request) throws RocksDBException, TarimKVException
    {
        validDeleteParam(request);
        String cfName = Integer.toString(request.getTableID());
        Slot slot = getSlot(request.getChunkID());
        ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);

        WriteOptions writeOpt = new WriteOptions();
        slot.delete(writeOpt, cfh, request.getKey());
    }

    /**
     */
    public List<KeyValue> prefixSeek(PrefixSeekRequest request) {
        return null;
    }

    /*------ chunk scan (only local) ------*/
     
    public KVSchema.PrepareScanInfo prepareChunkScan(String tableID, long[] chunks){
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        return null;
    }

    // ifComplete is output parameter, scan not complete until ifComplete == true.
    public List<KeyValue> deltaChunkScan(KVSchema.DeltaScanParam param, boolean ifComplete){ 
        return null;
    }

    // stop scan even ifComplete == false
    public void closeChunkScan(int snapshotID){
    }
}
