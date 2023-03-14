package com.deepexi.tarimdb.datamodels;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.tarimkv.KVMetadata;
import com.deepexi.tarimdb.tarimkv.Slot;

import io.grpc.stub.StreamObserver;
import com.deepexi.rpc.TarimMetaGrpc;
import com.deepexi.rpc.TarimProto;

import org.rocksdb.RocksDBException;

/**
 * TarimDBMeta
 *
 */
public class TarimDBMeta extends TarimMetaGrpc.TarimMetaImplBase {

    public final static Logger logger = LogManager.getLogger(TarimDBMeta.class);

    private KVMetadata metadata;
    Slot slot; //TODO: temporary implements, DB metadata save in a rocksdb instance.

    public TarimDBMeta(KVMetadata metadata) throws Exception, RocksDBException, IllegalArgumentException {
        super();
        this.metadata = metadata;
        this.slot = new Slot(metadata.metaSlotConf);
        slot.open();
        //logger.debug("TarimDBMeta constructor, metadata: " + metadata.toString());
    }

    public TarimDBMeta() {

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

