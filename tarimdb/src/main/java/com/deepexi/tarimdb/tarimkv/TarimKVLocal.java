package com.deepexi.tarimdb.tarimkv;

import java.util.List;
import java.util.Set;
//import java.util.String;
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
import com.deepexi.rpc.TarimKVProto.StatusResponse;
import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * TarimKVLocal
 *  
 *
 */
public class TarimKVLocal {

    public final static Logger logger = LogManager.getLogger(TarimKVLocal.class);

    //private List<TarimKVProto.Slot> slotsConf;
    private KVLocalMetadata lMetadata;
    private SlotManager slotManager;
    private TarimKVMetaClient metaClient;

    public TarimKVLocal(TarimKVMetaClient metaClient, KVLocalMetadata lMetadata) {
        this.metaClient = metaClient;
        this.lMetadata = lMetadata;
        logger.debug("TarimKVLocal constructor, local metadata: " + lMetadata.toString());
    }

    public void init(){
        slotManager.init(lMetadata.slots);
    }

    private StatusResponse getSlot(int chunkID, Slot slot){
        StatusResponse.Builder statusBuilder = StatusResponse.newBuilder();
        statusBuilder.setCode(Status.OK.getCode());
        statusBuilder.setMsg(Status.OK.getMsg());

        do{
            boolean refreshed = false;
            String slotID;
            do{
                slotID = metaClient.getMasterReplicaSlot(chunkID);
                KVLocalMetadata.Node node = metaClient.getReplicaNode(slotID);
                if(node.host != lMetadata.address || node.port != lMetadata.port){
                    if(refreshed == true){
                        statusBuilder.setCode(Status.DISTRIBUTION_ERROR.getCode());
                        statusBuilder.setMsg(Status.DISTRIBUTION_ERROR.getMsg());
                        break;
                    }
                    metaClient.refreshDistribution();
                    refreshed = true;
                }else{
                    break;
                }
            }while(true);

            slot = slotManager.getSlot(slotID);
            if(slot == null){
                statusBuilder.setCode(Status.MASTER_SLOT_NOT_FOUND.getCode());
                statusBuilder.setMsg(Status.MASTER_SLOT_NOT_FOUND.getMsg());
                break;
            }
        }while(false);

        return statusBuilder.build();
    } 

    private StatusResponse validPutParam(PutRequest request){
        StatusResponse.Builder statusBuilder = StatusResponse.newBuilder();
        statusBuilder.setCode(Status.OK.getCode());
        statusBuilder.setMsg(Status.OK.getMsg());
        if(request.getTableID() > 0
           && request.getChunkID() > 0 
           && request.getValuesCount() > 0) {
            return statusBuilder.build();
        }
        statusBuilder.setCode(Status.PARAM_ERROR.getCode());
        statusBuilder.setMsg(Status.PARAM_ERROR.getMsg());
        return statusBuilder.build();
    }
    /**
     */
    public StatusResponse put(PutRequest request) throws RocksDBException {
        StatusResponse status = validPutParam(request);
        if(status.getCode() != Status.OK.getCode()){
            return status;
        }
        Slot slot = null;
        status = getSlot(request.getChunkID(), slot);
        if(status.getCode() != Status.OK.getCode()){
            return status;
        }

        String cfName = Integer.toString(request.getTableID());
        slot.createColumnFamilyIfNotExist(cfName);

        // get db(rocksdb instance) // TODO: lock db 
        RocksDB db = slot.getDB();
        
        // writing key-values
        WriteOptions writeOpt = new WriteOptions();
        WriteBatch batch = new WriteBatch();
        //batch.put(String.getBytes("k"), String.getBytes("v"));
        batch.put("k".getBytes(), "v".getBytes());
        db.write(writeOpt, batch);

        return status;
    }

    /**
     */
    public List<KeyValue> get(GetRequest request) {
        return null;
    }

    /**
     */
    public List<KeyValue> prefixSeek(PrefixSeekRequest request) {
        return null;
    }

    /**
     */
    public void delete(DeleteRequest request) {
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
