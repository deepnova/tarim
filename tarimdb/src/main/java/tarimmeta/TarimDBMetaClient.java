package tarimmeta;

import com.deepexi.rpc.TarimGrpc;
import com.deepexi.rpc.TarimProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TarimDBMetaClient {
    private final ManagedChannel channel;
    private final TarimGrpc.TarimBlockingStub blockingStub;

    public TarimDBMetaClient(String host, int port) {
        //usePlaintext表示明文传输，否则需要配置ssl
        //channel  表示通信通道
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        //存根
        blockingStub = TarimGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public TarimProto.PrepareScanResponse prepareRequest(int tableID, List<String> partitionIDs){
        TarimProto.PrepareMetaNodeScanRequest request = TarimProto.PrepareMetaNodeScanRequest.newBuilder()
                .setTableID(tableID)
                .addAllPartitionID(partitionIDs)
                .build();
        TarimProto.PrepareScanResponse response = blockingStub.prepareScan(request);
        return response;
    }
}
