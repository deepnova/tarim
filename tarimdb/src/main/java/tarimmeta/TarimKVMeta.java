package tarimmeta;

//import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVMetaGrpc;
import com.deepexi.rpc.TarimKVProto.DataDistributionRequest;
import com.deepexi.rpc.TarimKVProto.DataDistributionResponse;
import com.deepexi.rpc.TarimKVProto.StatusResponse;

/**
 * TarimKVMeta
 *  TarimKV metadata server
 *
 */
public class TarimKVMeta extends TarimKVMetaGrpc.TarimKVMetaImplBase {

    public final static Logger logger = LogManager.getLogger(TarimKVMeta.class);

    private KVMetadata metadata;

    public TarimKVMeta(KVMetadata metadata) {
        super();
        this.metadata = metadata;
        logger.debug("TarimKVMeta constructor, metadata: " + metadata.toString());
    }

    public void getDataDistribution(DataDistributionRequest request,
                                    StreamObserver<DataDistributionResponse> responseObserver) {

        logger.debug("client request comes, metadata: " + metadata.toString());
        DataDistributionResponse.Builder respBuilder = DataDistributionResponse.newBuilder();
        StatusResponse.Builder statusBuilder = StatusResponse.newBuilder();
        statusBuilder.setCode(0);
        statusBuilder.setMsg("OK");
        respBuilder.setStatus(statusBuilder);
        respBuilder.setDistribution(metadata.toDistributionInfo(false));

        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }
}

