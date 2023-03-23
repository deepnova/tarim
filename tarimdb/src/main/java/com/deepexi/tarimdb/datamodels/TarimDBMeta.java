package com.deepexi.tarimdb.datamodels;

import com.deepexi.tarimdb.tarimkv.YamlLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.deepexi.tarimdb.tarimkv.KVMetadata;
import com.deepexi.tarimdb.tarimkv.Slot;
import io.grpc.stub.StreamObserver;
import com.deepexi.rpc.TarimMetaGrpc;
import com.deepexi.rpc.TarimProto;
import org.rocksdb.RocksDBException;

import java.io.InputStream;
import java.util.Scanner;

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
        respBuilder.setCode(0);
        respBuilder.setMsg("OK");
        respBuilder.setTableID(100);
        try {
            respBuilder.setTable(loadTableMeta());
        }catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The table meta is incorrect!");
        }

        respBuilder.setPrimaryKey("userID");
        respBuilder.addParitionKeys("class");

        TarimProto.PartitionSpec.Builder spec = TarimProto.PartitionSpec.newBuilder();
        TarimProto.Fields.Builder field = TarimProto.Fields.newBuilder();

        field.setName("class");
        field.setTransform("identity");
        field.setSourceID(2);
        field.setFiledID(1000);

        spec.addFields(0, field.build());
        spec.setSpecID(1);

        respBuilder.setPartitionSpec(spec);
        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }

    public void prepareScan(TarimProto.PrepareScanRequest request,
                            StreamObserver<TarimProto.PrepareScanResponse> responseObserver) 
    {
        //TODO
    }

    public String loadTableMeta(){
        InputStream inputStream = YamlLoader.class
                .getClassLoader()
                .getResourceAsStream("tablemeta.json");

        String jsonString = null;

        try (Scanner scanner = new Scanner(inputStream)) {
            jsonString = scanner.useDelimiter("\\A").next();
        }

        return jsonString;
    }
}

