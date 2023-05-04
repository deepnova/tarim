package tarimmeta;

import com.deepexi.rpc.TarimExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.deepexi.tarimdb.tarimkv.Slot;
import io.grpc.stub.StreamObserver;
import com.deepexi.rpc.TarimMetaGrpc;
import com.deepexi.rpc.TarimProto;
import org.rocksdb.RocksDBException;
import com.deepexi.tarimdb.util.Common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * TarimDBMeta
 *
 */
public class TarimDBMeta extends TarimMetaGrpc.TarimMetaImplBase {

    public final static Logger logger = LogManager.getLogger(TarimDBMeta.class);

    private KVMetadata metadata;
    private TarimMetaServer metaServer;

    Slot slot; //TODO: temporary implements, DB metadata save in a rocksdb instance.

    public TarimDBMeta(KVMetadata metadata) throws Exception, RocksDBException, IllegalArgumentException {
        super();
        this.metadata = metadata;
        //logger.debug("TarimDBMeta constructor, metadata: " + metadata.toString());
        this.metaServer = new TarimMetaServer(metadata);
    }

    public TarimDBMeta() {

    }

    public void getTable(TarimProto.GetTableRequest request, 
                         StreamObserver<TarimProto.GetTableResponse> responseObserver) 
    {
        logger.info("getTable() request: " + request.toString());

        //todo, the table meta should be from the DB
        if (request.getTblName().equals("tarim_table1")){
            TarimProto.GetTableResponse.Builder respBuilder = TarimProto.GetTableResponse.newBuilder();
            respBuilder.setCode(0);
            respBuilder.setMsg("OK");
            respBuilder.setTableID(100);
            try {
                respBuilder.setTable(Common.loadTableMeta("tablemeta.json"));
            }catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("The table meta is incorrect!");
            }

            respBuilder.addAllPrimaryKeys(Arrays.asList("userID"));
            respBuilder.addAllPartitionKeys(Arrays.asList("class"));

            TarimProto.PartitionSpec.Builder spec = TarimProto.PartitionSpec.newBuilder();
            TarimProto.Fields.Builder field = TarimProto.Fields.newBuilder();

            field.setName("class");
            field.setTransform("identity");
            field.setSourceID(3);
            field.setFiledID(1000);

            spec.addFields(0, field.build());
            spec.setSpecID(1);

            respBuilder.setPartitionSpec(spec);
            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        if (request.getTblName().equals("tarim_table2")){
            TarimProto.GetTableResponse.Builder respBuilder = TarimProto.GetTableResponse.newBuilder();
            respBuilder.setCode(0);
            respBuilder.setMsg("OK");
            respBuilder.setTableID(2);
            try {
                respBuilder.setTable(Common.loadTableMeta("table2meta.json"));
            }catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("The table meta is incorrect!");
            }

            respBuilder.addAllPrimaryKeys(Arrays.asList("userID", "number"));
            respBuilder.addAllPartitionKeys(Arrays.asList("class"));

            TarimProto.PartitionSpec.Builder spec = TarimProto.PartitionSpec.newBuilder();
            TarimProto.Fields.Builder field = TarimProto.Fields.newBuilder();

            field.setName("class");
            field.setTransform("identity");
            field.setSourceID(4);
            field.setFiledID(1000);

            spec.addFields(0, field.build());
            spec.setSpecID(1);

            respBuilder.setPartitionSpec(spec);
            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
            return;
        }
    }

    public void prepareScan(TarimProto.PrepareScanRequest request,
                            StreamObserver<TarimProto.PrepareScanResponse> responseObserver) {
        int tableID = request.getTableID();
        TarimExecutor.Executor executors = request.getExecutors(0);

        TarimExecutor.ExecType execType = executors.getExecType();
        byte[] conditions = executors.getSelection().getConditions().toByteArray();
        List<String> partitionIDs = executors.getPartitionScan().getPartitionIdsList();
        boolean allFlag = request.getAllPartition();

        //todo,  don't support projection now, columns no use
        List<String> columns  = executors.getPartitionScan().getColumnsList();
        TarimProto.PrepareScanResponse response = metaServer.prepareScan(tableID, allFlag, execType, conditions, partitionIDs);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void setPartition(TarimProto.PartitionRequest request,
                             StreamObserver<TarimProto.DbStatusResponse> responseObserver){

        int result = metaServer.setPartitionMsg(request.getTableID(), request.getPartitionID());

        TarimProto.DbStatusResponse response = TarimProto.DbStatusResponse.newBuilder().setCode(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}

