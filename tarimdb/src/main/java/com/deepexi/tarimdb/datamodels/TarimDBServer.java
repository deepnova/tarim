package com.deepexi.tarimdb.datamodels;

import com.deepexi.tarimdb.util.TarimKVException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.tarimkv.*;

import io.grpc.stub.StreamObserver;
import com.deepexi.rpc.TarimGrpc;
import com.deepexi.rpc.TarimProto;
import com.deepexi.rpc.TarimKVProto;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * TarimDB
 *
 */
public class TarimDBServer extends TarimGrpc.TarimImplBase {

    private TarimDB db;
    public final static Logger logger = LogManager.getLogger(TarimDB.class);

    public TarimDBServer(TarimDB db) {
        this.db = db;
    }

    public void insert(TarimProto.InsertRequest request,
                       StreamObserver<TarimProto.DbStatusResponse> responseObserver)
    {
        TarimProto.DbStatusResponse.Builder respBuilder = TarimProto.DbStatusResponse.newBuilder();

        String partitionID = request.getPartitoinID();
        int tableID = request.getTableID();
        byte[] record = request.getRecords().toByteArray();

        //todo the primaryKey from the connector now, or form the meta and make a check?
        String primaryKey = request.getPrimaryKey();
        //todo check the data, should not be null

        logger.info("TableID:{}, partitionID:{}" , tableID , partitionID);

        int result = db.insertMsgProc(tableID, partitionID, primaryKey, record);
        if (result != 0){
            respBuilder.setCode(1);
            respBuilder.setMsg("insert data to rocksDb err!");
        }else{
            respBuilder.setCode(0);
        }

        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }

    public void lookup(TarimProto.LookupRequest request,
                       StreamObserver<TarimProto.LookupResponse> responseObserver) 
    {
        //TODO
    }

    public void prepareScan(TarimProto.PrepareMetaNodeScanRequest request,
                            StreamObserver<TarimProto.PrepareScanResponse> responseObserver)
    {
        int tableID = request.getTableID();
        List<String> partitionIDs = request.getPartitionIDList();
        TarimProto.PrepareScanResponse response = null;
        try {
            response = db.preScan(tableID, partitionIDs);
        } catch (TarimKVException e) {
            throw new RuntimeException(e);
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void scan(TarimProto.ScanRequest request,
                     StreamObserver<TarimProto.ScanResponse> responseObserver) 
    {
        int tableID = request.getTableID();
        long scanHandle = request.getScanHandler();
        String partitionID = request.getPartitionID();
        TarimProto.ScanResponse response = null;

        try {
            response = db.scanMsgProc(tableID, scanHandle, partitionID);
        } catch (TarimKVException e) {
            throw new RuntimeException(e);
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}

