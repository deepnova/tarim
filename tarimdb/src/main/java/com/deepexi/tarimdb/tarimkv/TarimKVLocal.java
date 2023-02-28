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

    private Slot getSlot(long chunkID) throws TarimKVException {
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

    private void validPrefixSeekParam(PrefixSeekRequest request) throws TarimKVException
    {
        if(request.getTableID() > 0
           && request.getChunkID() > 0 
           //&& request.getScanSize() >= 0 
           && request.getPrefix() != null && !request.getPrefix().isEmpty()) 
        {
           return;
        }
        throw new TarimKVException(Status.PARAM_ERROR);
    }

    /**
     * only supported prefix seeks in a chunk, and return all result one time.
     */
    public List<TarimKVProto.KeyValue> prefixSeek(PrefixSeekRequest request) 
            throws RocksDBException, TarimKVException
    {
        validPrefixSeekParam(request);
        String cfName = Integer.toString(request.getTableID());
        Slot slot = getSlot(request.getChunkID());
        ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);

        String keyPrefix = KeyValueCodec.KeyPrefixEncode(request.getChunkID(), request.getPrefix());
        ReadOptions readOpt = new ReadOptions();
        List<TarimKVProto.KeyValue> values = slot.prefixSeek(readOpt, cfh, keyPrefix);
        logger.info("prefixSeek(), chunkID: " + request.getChunkID()
                  + ", key prefix: " + keyPrefix
                  + ", result size: " + values.size());
        return values;
    }

    /*------ chunk scan (only local) ------*/
     
    public KVSchema.PrepareScanInfo prepareChunkScan(int tableID, long[] chunks) throws TarimKVException, RocksDBException
    {
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        if(tableID <= 0 || chunks.length <= 0)
        {
            throw new TarimKVException(Status.PARAM_ERROR);
        }
        KVSchema.PrepareScanInfo scanInfo = new KVSchema.PrepareScanInfo();
        String cfName = Integer.toString(tableID);
        for(int i = 0; i < chunks.length; i++)
        {
            Slot slot = getSlot(chunks[i]);
            ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);
            KVSchema.ChunkDetail chunkDetail = new KVSchema.ChunkDetail();
            chunkDetail.chunkID = chunks[i];
            // get current snapshot, and keep it in memory until scan stop
            chunkDetail.snapshotID = slot.prepareScan(cfh);
            chunkDetail.mergePolicy = 1;
            chunkDetail.mainPath = lMetadata.mainPath; //TODO: may too long
            //TODO: different chunks may in same slot, should return the same snapshot
        }

        scanInfo.mainAccount = lMetadata.mainAccount;

        return scanInfo;
    }

    private void validDeltaChunkScanParam(KVSchema.DeltaScanParam param) throws TarimKVException
    {
        if( (param.scope == 1 || param.scope == 2)
           && param.tableID > 0 
           && param.snapshotID >= 0 // TODO
           && param.chunkID > 0 
           /*&& param.scanSize > 0*/)
        {
           return;
        }
        throw new TarimKVException(Status.PARAM_ERROR);
    }

    // ifComplete is output parameter, scan not complete until ifComplete == true.
    public List<TarimKVProto.KeyValue> deltaChunkScan(KVSchema.DeltaScanParam param, boolean ifComplete)
            throws RocksDBException, TarimKVException
    {
        validDeltaChunkScanParam(param);
        String cfName = Integer.toString(param.tableID);
        Slot slot = getSlot(param.chunkID);
        ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);

        // lastKey
        // scanSize

        return null;
    }

    // stop scan even ifComplete == false
    public void closeChunkScan(int tableID, int snapshotID, long[] chunks)
    {
        // do nothing in the prototype
        //slot.releaseSnapshot(long snapshotID) 
    }
}
