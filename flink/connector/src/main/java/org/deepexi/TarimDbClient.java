package org.deepexi;

import com.deepexi.rpc.TarimGrpc;
import com.deepexi.rpc.TarimMetaGrpc;
import com.deepexi.rpc.TarimProto;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class TarimDbClient {
    private final ManagedChannel channel;
    private final TarimGrpc.TarimBlockingStub blockingStub;

    public TarimDbClient(String host, int port) {
        //usePlaintext表示明文传输，否则需要配置ssl
        //channel  表示通信通道
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        //存根
        blockingStub = TarimGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public int insertRequest(int tableID, String partitionData, byte[] record, String primaryKey){
        TarimProto.InsertRequest request = TarimProto.InsertRequest.newBuilder()
                .setTableID(tableID)
                .setPartitoinID(partitionData)
                .setRecords(ByteString.copyFrom(record))
                .setPrimaryKey(primaryKey)
                .build();
        TarimProto.DbStatusResponse response = blockingStub.insert(request);
        return response.getCode();
    }

    public TarimProto.ScanResponse scanRequest(int tableID, long handle, String partitionID, String planID,
                                               int scanSize, String lowerBound, String upperBound, int lowerBoundType, int upperBoundType){
        TarimProto.ScanRequest request = TarimProto.ScanRequest.newBuilder()
                .setTableID(tableID)
                .setPartitionID(partitionID)
                .setScanHandler(handle)
                .setPlanID(planID)
                .setScanSize(scanSize)
                .setLowerBound(lowerBound)
                .setUpperBound(upperBound)
                .setLowerBoundType(lowerBoundType)
                .setUpperBoundType(upperBoundType)
                .build();

        TarimProto.ScanResponse response = blockingStub.scan(request);
        return response;
    }
}
