package com.deepexi.tarimdb.tarimkv;

import com.deepexi.TarimMetaClient;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVProto.StatusResponse;

import com.deepexi.rpc.TarimKVProto.*;
import com.deepexi.rpc.TarimKVGrpc;
import tarimmeta.KVMetadata;

/**
 * TarimKVRemote
 *  Server of business metadata 
 *  Run with TarimKVMeta (How about run with DataNode?)
 *
 */
public class TarimKVRemote extends TarimKVGrpc.TarimKVImplBase {

    public final static Logger logger = LogManager.getLogger(TarimKVRemote.class);

    private KVMetadata metadata;
    private TarimMetaClient metaClient;
    //private List<TarimKVProto.Slot> slotsConf;
    private KVLocalMetadata lMetadata;
    private TarimKVLocal kvLocal;

    public TarimKVRemote(KVMetadata metadata, KVLocalMetadata lMetadata) {
        this.metadata = metadata;
        this.lMetadata = lMetadata;
        logger.debug("TarimKVRemote constructor, metadata: " + metadata.toString());
        logger.debug("TarimKVLocal constructor, local metadata: " + lMetadata.toString());

        metaClient = new TarimMetaClient(metadata.toDistributionInfo(false));
        kvLocal = new TarimKVLocal(metaClient, lMetadata);
    }

    /**
     */
    public void put(PutRequest request, StreamObserver<StatusResponse> responseObserver) {
        // kvLocal.put();
    }

    /**
     */
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    }

    /**
     */
    public void prefixSeek(PrefixSeekRequest request, StreamObserver<GetResponse> responseObserver) {
    }

    /**
     */
    public void delete(DeleteRequest request, StreamObserver<StatusResponse> responseObserver) {
    }
}
