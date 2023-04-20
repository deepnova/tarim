package com.deepexi.tarimdb.tarimkv;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Arrays;

import com.deepexi.KvNode;
import com.deepexi.TarimMetaClient;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
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
    private TarimMetaClient metaClient;

    public TarimKVLocal(TarimMetaClient metaClient, KVLocalMetadata lMetadata) {
        this.metaClient = metaClient;
        this.lMetadata = lMetadata;
        slotManager = new SlotManager();
        logger.debug("TarimKVLocal constructor, local metadata: " + lMetadata.toString());
    }

    public void init() throws Exception, IllegalArgumentException, RocksDBException {
        slotManager.init(lMetadata.slots);
    }

    private Slot getSlot(long chunkID) throws TarimKVException {
        boolean refreshed = false;
        String slotID;
        logger.info("lMetadata: " + lMetadata.toString());
        do{
            slotID = metaClient.getMasterReplicaSlot(chunkID);
            logger.info("chunkID: " + chunkID + ", slotID: " + slotID);
            if(slotID == null) throw new TarimKVException(Status.NULL_POINTER);
            KvNode node = metaClient.getReplicaNode(slotID);
            if(node == null) throw new TarimKVException(Status.NULL_POINTER);
            logger.info("get replica node: " + node.toString());
            if(!node.host.equals(lMetadata.address)  || node.port != lMetadata.port){
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
        logger.info("PutRequest param, tableID" + request.getTableID()
                  + ", chunkID: " + request.getChunkID()
                  + ", values count: " + request.getValuesCount());
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
        for(TarimKVProto.KeyValueByte kv : request.getValuesList()){
            key = KeyValueCodec.KeyEncode( new KeyValueCodec(request.getChunkID(), kv) );
            batch.put(cfh
                      ,key.getBytes()
                      , kv.getValue().toByteArray());
            logger.info("for WriteBatch, key: " + key + ", value: " + kv.getValue());
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

        List<String> internalKeys = new ArrayList<>();
        for(String key : request.getKeysList())
        {
            String key0 = KeyValueCodec.KeyEncode(request.getChunkID(), key);
            internalKeys.add(key0);
        }
        logger.info("get(), internal keys: " + internalKeys.toString());
        ReadOptions readOpt = new ReadOptions();
        List<byte[]> values = slot.multiGet(readOpt, cfh, internalKeys);

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

        String key = KeyValueCodec.KeyEncode(request.getChunkID(), request.getKey());
        logger.info("delete(), internal key: " + key);
        WriteOptions writeOpt = new WriteOptions();
        slot.delete(writeOpt, cfh, key);
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
    public List<TarimKVProto.KeyValueByte> prefixSeek(PrefixSeekRequest request)
            throws RocksDBException, TarimKVException
    {
        validPrefixSeekParam(request);
        String cfName = Integer.toString(request.getTableID());
        Slot slot = getSlot(request.getChunkID());
        ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);

        String keyPrefix = KeyValueCodec.KeyPrefixEncode(request.getChunkID(), request.getPrefix());
        ReadOptions readOpt = new ReadOptions();
        List<TarimKVProto.KeyValueByte> values = slot.prefixSeek(readOpt, cfh, keyPrefix);
        logger.info("prefixSeek(), chunkID: " + request.getChunkID()
                  + ", key prefix: " + keyPrefix
                  + ", result size: " + values.size());
        return values;
    }

    /*------ chunk scan (only local) ------*/
     
    public KVSchema.PrepareScanInfo prepareChunkScan(int tableID, long[] chunks) throws TarimKVException, RocksDBException
    {
        //TODO: open iterator instead of snapshotID and return iterator handler, maybe better
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        if(tableID <= 0 || chunks.length <= 0)
        {
            throw new TarimKVException(Status.PARAM_ERROR);
        }
        KVSchema.PrepareScanInfo scanInfo = new KVSchema.PrepareScanInfo();
        scanInfo.chunkDetails = new ArrayList<>();
        String cfName = Integer.toString(tableID);
        for(int i = 0; i < chunks.length; i++)
        {
            Slot slot = getSlot(chunks[i]);
            ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);
            KVSchema.ChunkDetail chunkDetail = new KVSchema.ChunkDetail();
            chunkDetail.chunkID = chunks[i];
            // get current snapshot, and keep it in memory until scan stop
            String keyPrefix = KeyValueCodec.ChunkOnlyKeyPrefixEncode(chunks[i]);
            chunkDetail.scanHandler = slot.prepareScan(cfh, keyPrefix);
            chunkDetail.mergePolicy = 1;
            chunkDetail.mainPaths.add(lMetadata.mainPath); //TODO: may too long
            //TODO: different chunks may in same slot, should return the same snapshot
            logger.info("prepareChunkScan(), ColumnFamilyHandle name: " + cfh.getName() 
                      + ", chunkID: " + chunks[i]
                      + ", scanHandler: " + chunkDetail.scanHandler
                      + ", mainPaths: " + chunkDetail.mainPaths.toString());
            scanInfo.chunkDetails.add(chunkDetail);
        }

        scanInfo.mainAccount = lMetadata.mainAccount;

        logger.info("prepareChunkScan(), tableID: " + tableID
                  + ", chunks: " + Arrays.toString(chunks)
                  + ", mainAccount: " + lMetadata.mainAccount.toString());
        logger.info("prepareChunkScan(), scanInfo: " + scanInfo.toString());

        return scanInfo;
    }

    private void validDeltaChunkScanParam(KVSchema.DeltaScanParam param) throws TarimKVException
    {
        if( (param.scope == 1 || param.scope == 2)
           && param.tableID > 0 
           && param.scanHandler >= 0 // TODO
           && param.chunkID > 0 
           /*&& param.scanSize > 0*/)
        {
           return;
        }
        throw new TarimKVException(Status.PARAM_ERROR);
    }

    // ifComplete is output parameter, scan not complete until ifComplete == true.
    public TarimKVProto.RangeData deltaChunkScan(KVSchema.DeltaScanParam param, boolean ifComplete)
            throws RocksDBException, TarimKVException
    {
        //TODO: support lastKey and scanSize for full scan
        ifComplete = true;

        validDeltaChunkScanParam(param);
        String cfName = Integer.toString(param.tableID);
        Slot slot = getSlot(param.chunkID);
        ColumnFamilyHandle cfh = slot.getColumnFamilyHandle(cfName);
        String startKey = KeyValueCodec.ChunkOnlyKeyPrefixEncode(param.chunkID);
        ReadOptions readOpts = new ReadOptions();
        String lowerBoundKey = KeyValueCodec.KeyPrefixEncode(param.chunkID, param.lowerBound);
        String upperBoundKey = KeyValueCodec.KeyPrefixEncode(param.chunkID, param.upperBound);

        TarimKVProto.RangeData results = slot.deltaScan(readOpts, cfh, param.scanHandler, startKey, param.scanSize,
                param.planID, lowerBoundKey, upperBoundKey, param.lowerBoundType, param.upperBoundType);
        return results;
    }

    // stop scan even ifComplete == false
    public void closeChunkScan(int tableID, KVSchema.ChunkScanHandler[] scanHandlers) throws TarimKVException
    {
        String cfName = Integer.toString(tableID);
        for(KVSchema.ChunkScanHandler handler : scanHandlers)
        {
            Slot slot = getSlot(handler.chunkID);
            slot.releaseScanHandler(handler.scanHandler);
        }
    }

    public void closeChunkScan(int tableID, long chunkID, long scanHandler) throws TarimKVException
    {
        String cfName = Integer.toString(tableID);
        Slot slot = getSlot(chunkID);
        slot.releaseScanHandler(scanHandler);
    }
}
