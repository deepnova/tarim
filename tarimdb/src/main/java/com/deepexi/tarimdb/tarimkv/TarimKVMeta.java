package com.deepexi.tarimdb.tarimkv;

import java.util.Iterator;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVGrpc;
import com.deepexi.rpc.TarimKVMetaSvc.DataDistributionRequest;
import com.deepexi.rpc.TarimKVMetaSvc.DataDistributionResponse;
import com.deepexi.rpc.TarimKVMetaSvc.DistributionInfo;
import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;

/**
 * TarimKVMeta
 *  TarimKV metadata server
 *
 */
public class TarimKVMeta extends TarimKVGrpc.TarimKVImplBase {

    public final static Logger logger = LogManager.getLogger(TarimKVMeta.class);

    private KVMetadata metadata; // context

    public TarimKVMeta(KVMetadata metadata) {
        super();
        this.metadata = metadata;
        logger.debug("TarimKVMeta constructor, metadata: " + metadata.toString());
    }

    public static<T> Iterable<T> iteratorToIterable(Iterator<T> iterator)
    {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return iterator;
            }
        };
    }

/*
    public void sendUnaryRequest(TarimKVMetaSvc.UnaryRequest request,
                                 StreamObserver<TarimKVMetaSvc.UnaryResponse> responseObserver) {
        //TODO: Demo
        ByteString message = request.getData();
        logger.debug("server, serviceName:" + request.getServiceName() 
                 + "; methodName:" + request.getMethodName()
                 + "; datas:" + new String(message.toByteArray()) );

        TarimKVMetaSvc.UnaryResponse.Builder builder = TarimKVMetaSvc.UnaryResponse.newBuilder();
        builder.setServiceName("GrpcServiceResponse").setMethodName("sendUnaryResponse");

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
*/
    public void getDataDistribution(DataDistributionRequest request,
                                    StreamObserver<DataDistributionResponse> responseObserver) {

        logger.debug("client request comes, metadata: " + metadata.toString());
        DataDistributionResponse.Builder respBuilder = DataDistributionResponse.newBuilder();
        DistributionInfo.Builder distBuilder = DistributionInfo.newBuilder();
        distBuilder.addAllRgroups(iteratorToIterable(metadata.rgroups.iterator()));
        distBuilder.addAllDnodes(iteratorToIterable(metadata.dnodes.iterator()));

        respBuilder.setCode(0);
        respBuilder.setMsg("OK");
        respBuilder.setDistribution(distBuilder);

        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }
}

