package org.deepexi;
import com.deepexi.rpc.TarimProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

import com.deepexi.rpc.*;
public class MetaClient {
    private final ManagedChannel channel;
    private final TarimMetaGrpc.TarimMetaBlockingStub blockingStub;
    private static final String host = "127.0.0.1";
    private static final int port = 1301;

    public MetaClient(String host, int port) {
        //usePlaintext表示明文传输，否则需要配置ssl
        //channel  表示通信通道
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        //存根
        blockingStub = TarimMetaGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public TarimProto.GetTableResponse loadTableRequest(String catalogName, String dataBaseName, String tableName){
        TarimProto.GetTableRequest request = TarimProto.GetTableRequest.newBuilder()
                .setCatName(catalogName)
                .setDbName(dataBaseName)
                .setTblName(tableName)
                .build();
        TarimProto.GetTableResponse response = blockingStub.getTable(request);
        return response;
    }
}
