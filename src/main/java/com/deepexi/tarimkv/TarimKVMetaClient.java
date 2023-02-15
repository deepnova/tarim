package com.deepexi.tarimkv;

import com.deepexi.util.TLog;
import com.deepexi.util.Status;
import com.deepexi.rpc.TarimKVMetaSvc.DataDistributionRequest;
import com.deepexi.rpc.TarimKVMetaSvc.DataDistributionResponse;
import com.deepexi.rpc.TarimKVMetaSvc.DistributionInfo;

import java.util.concurrent.TimeUnit;
import com.deepexi.rpc.TarimKVGrpc;
import com.deepexi.rpc.TarimKVMetaSvc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * TarimKVMetaClient
 *  TarimKV metadata client
 *  kv
 */
public class TarimKVMetaClient {

    private final ManagedChannel channel;//客户端与服务器的通信channel
    private final TarimKVGrpc.TarimKVBlockingStub blockStub;//阻塞式客户端存根节点

    public TarimKVMetaClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();//指定grpc服务器地址和端口初始化通信channel
        blockStub = TarimKVGrpc.newBlockingStub(channel);//根据通信channel初始化客户端存根节点
    }

    public void shutdown() throws InterruptedException{
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    //客户端方法
    public void sayHello(String str){
        //封装请求参数
        TarimKVMetaSvc.UnaryRequest request = TarimKVMetaSvc.UnaryRequest.newBuilder()
                                                          .setServiceName("GrpcServiceRequest")
                                                          .setMethodName("sendUnaryRequest")
                                                          .setData(ByteString.copyFrom(str.getBytes()))
                                                          .build();
        //客户端存根节点调用grpc服务接口，传递请求参数
        TarimKVMetaSvc.UnaryResponse response = blockStub.sendUnaryRequest(request);

        TLog.debug("client, serviceName:"+response.getServiceName()+"; methodName:"+response.getMethodName());
    }

    public DistributionInfo getDistribution(){

        DataDistributionRequest request = DataDistributionRequest.newBuilder().setTableID(1).build();
        DataDistributionResponse response = blockStub.getDataDistribution(request);

        TLog.debug("get distribution code: " + response.getCode());
        for(TarimKVMetaSvc.Node node : response.getDistribution().getDnodesList()){
            TLog.debug("distribution node: "+ KVMetadata.ObjToString(node));
        }

        for(TarimKVMetaSvc.RGroupItem rg: response.getDistribution().getRgroupsList()){
            TLog.debug("distribution rgroup: "+ KVMetadata.ObjToString(rg));
        }

        return response.getDistribution();
    }
}
