package com.deepexi.tarimdb.tarimkv;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.rpc.TarimKVMetaSvc.DataDistributionRequest;
import com.deepexi.rpc.TarimKVMetaSvc.DataDistributionResponse;
import com.deepexi.rpc.TarimKVMetaSvc.DistributionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
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

    // TODO: not thread safety, need lock
        
    public final static Logger logger = LogManager.getLogger(TarimKVMetaClient.class);

    private String metaHost;
    private int metaPort;
    private DistributionInfo dataDist;
    private HashMap<String, KVLocalMetadata.Node> mapSlotsNodes; // <slot:id, Node>

    public TarimKVMetaClient(String host, int port) {
        metaHost = host;
        metaPort = port;
        //refreshDistribution();
    }
/*
    public void shutdown() throws InterruptedException{
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
*/
    public void refreshDistribution() {

        ManagedChannel channel;//客户端与服务器的通信channel
        TarimKVGrpc.TarimKVBlockingStub blockStub;//阻塞式客户端存根节点
        channel = ManagedChannelBuilder.forAddress(metaHost, metaPort).usePlaintext().build();//指定grpc服务器地址和端口初始化通信channel
        blockStub = TarimKVGrpc.newBlockingStub(channel);//根据通信channel初始化客户端存根节点

        DataDistributionRequest request = DataDistributionRequest.newBuilder().setTableID(1).build();
        DataDistributionResponse response = blockStub.getDataDistribution(request);

        logger.debug("get distribution code: " + response.getCode());
        for(TarimKVMetaSvc.Node node : response.getDistribution().getDnodesList()){
            logger.debug("distribution node: "+ KVMetadata.ObjToString(node));
        }

        for(TarimKVMetaSvc.RGroupItem rg: response.getDistribution().getRgroupsList()){
            logger.debug("distribution rgroup: "+ KVMetadata.ObjToString(rg));
        }
        dataDist = response.getDistribution();
        channel.shutdown();

        // must re-build map
        if(mapSlotsNodes == null) mapSlotsNodes = new HashMap<String, KVLocalMetadata.Node>();
        else mapSlotsNodes.clear();
        for(TarimKVMetaSvc.Node node: dataDist.getDnodesList()) {
            if(node.getHost().isEmpty() || node.getPort() == 0) continue;
            for(TarimKVMetaSvc.Slot slot : node.getSlotsList()){
                if(slot.getId().isEmpty()) continue;
                mapSlotsNodes.put(slot.getId(), new KVLocalMetadata.Node(node.getHost(), node.getPort()));
            }
        }
    }

    public DistributionInfo getDistribution(){
        if(dataDist == null) {
            refreshDistribution();
        }
        return dataDist;
    }

    public KVLocalMetadata.Node getMasterReplicaNode(TarimKVMetaSvc.RGroupItem rgroup){
        for(TarimKVMetaSvc.Slot slot : rgroup.getSlotsList()){
            if(slot.getRole() == TarimKVMetaSvc.SlotRole.SR_MASTER){
                KVLocalMetadata.Node node = mapSlotsNodes.get(slot.getId());
                if(node == null){
                    logger.error("slot:" + slot.getId() + " not foun node, rgroup:" 
                                 + rgroup.getId() + "(hashValue:" + rgroup.getHashValue() + ")");
                    continue;
                }
                logger.debug("rgroup:" + rgroup.getId() + "(hashValue:" + rgroup.getHashValue()
                           + "), master slot:" + slot.getId() 
                           + ", host: " + node.host
                           + ", port: " + node.port);
                return new KVLocalMetadata.Node(node);
            }
        }
        logger.error("rgroup:" + rgroup.getId() + "(hashValue:" + rgroup.getHashValue() + ") not found data node.");
        return null;
    }

    public KVLocalMetadata.Node getReplicaNode(long hashValue) {
        if(dataDist == null) {
            refreshDistribution();
        }
        List<TarimKVMetaSvc.RGroupItem> rgroups = dataDist.getRgroupsList();
        if(rgroups.size() == 0){
            logger.error("fatal error, not found any rgroup, that means no data node.");
            return null;
        }else if(rgroups.size() == 1){
            return getMasterReplicaNode(rgroups.get(0));
        }else{
            TarimKVMetaSvc.RGroupItem last;
            TarimKVMetaSvc.RGroupItem curr;
            for(int i = 1; i < rgroups.size(); i++)
            {
                last = rgroups.get(i-1);
                curr = rgroups.get(i);
                if(last.getHashValue() < hashValue && hashValue <= curr.getHashValue()){
                    return getMasterReplicaNode(curr);
                }
            }
            return getMasterReplicaNode(rgroups.get(0)); // 所有节点看成一个环，找不到的hash值即分布在第一个节点上。
        }
    }
}
