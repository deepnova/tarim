package com.deepexi.tarimdb.datamodels;

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

        System.out.println("TableID:" + tableID);
        System.out.println("partitionID:" + partitionID);


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
        //TODO
    }

    public void scan(TarimProto.ScanRequest request,
                     StreamObserver<TarimProto.ScanResponse> responseObserver) 
    {
        //TODO
    }
}

