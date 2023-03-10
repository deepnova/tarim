package com.deepexi.tarimdb.datamodels;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.tarimkv.*;

import io.grpc.stub.StreamObserver;
import com.deepexi.rpc.TarimGrpc;
import com.deepexi.rpc.TarimProto;
import com.deepexi.rpc.TarimKVProto;

/**
 * TarimDB
 *
 */
public class TarimDBServer extends TarimGrpc.TarimImplBase {

    public final static Logger logger = LogManager.getLogger(TarimDB.class);

    public TarimDBServer() {
    }

    public void insert(TarimProto.InsertRequest request,
                       StreamObserver<TarimKVProto.StatusResponse> responseObserver) 
    {
        //TODO
    }

    public void lookup(TarimProto.LookupRequest request,
                       StreamObserver<TarimProto.LookupResponse> responseObserver) 
    {
        //TODO
    }

    public void prepareScan(TarimProto.PrepareScanRequest request,
                            StreamObserver<TarimProto.PrepareScanResponse> responseObserver) 
    {
        //TODO
    }

    public void scan(TarimProto.ScanRequest request,
                     StreamObserver<TarimProto.ScanResponse> responseObserver) 
    {
        //TODO
    }
}

