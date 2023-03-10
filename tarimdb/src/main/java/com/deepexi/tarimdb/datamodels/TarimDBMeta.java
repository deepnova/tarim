package com.deepexi.tarimdb.datamodels;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.tarimkv.KVMetadata;

import io.grpc.stub.StreamObserver;
import com.deepexi.rpc.TarimMetaGrpc;
import com.deepexi.rpc.TarimProto;

/**
 * TarimDBMeta
 *
 */
public class TarimDBMeta extends TarimMetaGrpc.TarimMetaImplBase {

    public final static Logger logger = LogManager.getLogger(TarimDBMeta.class);

    //private KVMetadata metadata;

    public TarimDBMeta(/*KVMetadata metadata*/) {
        super();
        //this.metadata = metadata;
        //logger.debug("TarimDBMeta constructor, metadata: " + metadata.toString());
    }

    public void getTable(TarimProto.GetTableRequest request, 
                         StreamObserver<TarimProto.GetTableResponse> responseObserver) 
    {
        logger.info("getTable() request: " + request.toString());

        TarimProto.GetTableResponse.Builder respBuilder = TarimProto.GetTableResponse.newBuilder();
        respBuilder.setCode(1);
        respBuilder.setMsg("OK");
        respBuilder.setTableID(100);
        respBuilder.setTable("{\"name\": \"some name\"}");

        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }

    public void prepareScan(TarimProto.PrepareScanRequest request,
                            StreamObserver<TarimProto.PrepareScanResponse> responseObserver) 
    {
        //TODO
    }
}

