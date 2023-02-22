package com.deepexi.tarimdb.tarimkv;

import java.util.List;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVMetaGrpc;
import com.deepexi.rpc.TarimKVProto.DataDistributionRequest;
import com.deepexi.rpc.TarimKVProto.DataDistributionResponse;
import com.deepexi.rpc.TarimKVProto.DistributionInfo;
import com.deepexi.rpc.TarimKVProto.StatusResponse;
import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;

import com.deepexi.rpc.TarimKVProto.*;
import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimKVGrpc;

/**
 * TarimKVRemote
 *  Server of business metadata 
 *  Run with TarimKVMeta
 *
 */
public class TarimKVRemote extends TarimKVGrpc.TarimKVImplBase {

    public final static Logger logger = LogManager.getLogger(TarimKVRemote.class);

    private KVMetadata metadata;
    private TarimKVMetaClient metaClient;
    //private List<TarimKVProto.Slot> slotsConf;
    private KVLocalMetadata lMetadata;
    private TarimKVLocal kvLocal;

    public TarimKVRemote(KVMetadata metadata, KVLocalMetadata lMetadata) {
        this.metadata = metadata;
        this.lMetadata = lMetadata;
        logger.debug("TarimKVRemote constructor, metadata: " + metadata.toString());
        logger.debug("TarimKVLocal constructor, local metadata: " + lMetadata.toString());

        metaClient = new TarimKVMetaClient(metadata.toDistributionInfo(false));
        kvLocal = new TarimKVLocal(metaClient, lMetadata);
    }

    /**
     */
    public void put(PutRequest request, StreamObserver<StatusResponse> responseObserver) {
        // kvLocal.put();
    }

    /**
     */
    public void get(GetRequest request, StreamObserver<GetReponse> responseObserver) {
    }

    /**
     */
    public void prefixSeek(PrefixSeekRequest request, StreamObserver<GetReponse> responseObserver) {
    }

    /**
     */
    public void delete(DeleteRequest request, StreamObserver<StatusResponse> responseObserver) {
    }
}
