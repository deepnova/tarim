package com.deepexi;

import com.deepexi.rpc.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.codec.digest.MurmurHash3;

public class TarimMetaClient implements Serializable {
    // TODO: not thread safety, need lock

    public final static Logger logger = LogManager.getLogger(TarimMetaClient.class);
    private String metaHost;
    private int test = 0;
    private int metaPort;
    private TarimKVProto.DistributionInfo dataDist;
    private HashMap<String, KvNode> mapSlotsNodes; // <slot:id, Node>

    private static final int ACCESS_REMOTE = 1;
    private static final int ACCESS_LOCAL = 2;
    private int accessMode; // 1: remote, 2: local

    public TarimMetaClient(String host, int port) {
        metaHost = host;
        metaPort = port;
        accessMode = ACCESS_REMOTE;
    }

    public TarimMetaClient(TarimKVProto.DistributionInfo dataDist) {
        this.dataDist = dataDist;
        accessMode = ACCESS_LOCAL;
    }

    public void refreshDistribution(TarimKVProto.DistributionInfo dataDist) {
        this.dataDist = dataDist;
        rebuildMap();
    }

    public void refreshDistribution() {

        ManagedChannel channel;//客户端与服务器的通信channel
        TarimKVMetaGrpc.TarimKVMetaBlockingStub blockStub;//阻塞式客户端存根节点
        channel = ManagedChannelBuilder.forAddress(metaHost, metaPort).usePlaintext().build();//指定grpc服务器地址和端口初始化通信channel
        blockStub = TarimKVMetaGrpc.newBlockingStub(channel);//根据通信channel初始化客户端存根节点

        TarimKVProto.DataDistributionRequest request = TarimKVProto.DataDistributionRequest.newBuilder().setTableID(1).build();
        TarimKVProto.DataDistributionResponse response = blockStub.getDataDistribution(request);

        logger.debug("get distribution code: " + response.getStatus().getCode());
        /*
        for(TarimKVProto.Node node : response.getDistribution().getDnodesList()){
            logger.debug("distribution node: "+ KVMetadata.ObjToString(node));
        }

        for(TarimKVProto.RGroupItem rg: response.getDistribution().getRgroupsList()){
            logger.debug("distribution rgroup: "+ KVMetadata.ObjToString(rg));
        }
         */
        dataDist = response.getDistribution();
        channel.shutdown();
        rebuildMap();
    }

    private void rebuildMap(){
        // must re-build map
        if(mapSlotsNodes == null) mapSlotsNodes = new HashMap<String, KvNode>();
        else mapSlotsNodes.clear();
        for(TarimKVProto.Node node: dataDist.getDnodesList()) {
            if(node.getHost().isEmpty() || node.getPort() == 0) continue;
            for(TarimKVProto.Slot slot : node.getSlotsList()){
                if(slot.getId().isEmpty()) continue;
                mapSlotsNodes.put(slot.getId(), new KvNode(node.getHost(), node.getPort()));
            }
        }
    }

    public TarimKVProto.DistributionInfo getDistribution(){
        if(dataDist == null) {
            refreshDistribution();
        }
        return dataDist;
    }

    public KvNode getMasterReplicaNode(TarimKVProto.RGroupItem rgroup){
        for(TarimKVProto.Slot slot : rgroup.getSlotsList()){
            if(slot.getRole() == TarimKVProto.SlotRole.SR_MASTER){
                KvNode node = mapSlotsNodes.get(slot.getId());
                if(node == null){
                    logger.error("slot:" + slot.getId() + " not foun node, rgroup:"
                            + rgroup.getId() + "(hashValue:" + rgroup.getHashValue() + ")");
                    continue;
                }
                logger.debug("rgroup:" + rgroup.getId() + "(hashValue:" + rgroup.getHashValue()
                        + "), master slot:" + slot.getId()
                        + ", host: " + node.host
                        + ", port: " + node.port);
                return new KvNode(node);
            }
        }
        logger.error("rgroup:" + rgroup.getId() + "(hashValue:" + rgroup.getHashValue() + ") not found master data node.");
        return null;
    }

    public KvNode getMasterReplicaNode(long hashValue) {
        if(dataDist == null) {
            refreshDistribution();
        }
        List<TarimKVProto.RGroupItem> rgroups = dataDist.getRgroupsList();
        if(rgroups.size() == 0){
            logger.error("fatal error, not found any rgroup, that means no data node.");
            return null;
        }else if(rgroups.size() == 1){
            return getMasterReplicaNode(rgroups.get(0));
        }else{
            TarimKVProto.RGroupItem last;
            TarimKVProto.RGroupItem curr;
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

    public KvNode getMasterReplicaNode(int chunkID) {
        long hash = MurmurHash3.hash32((long)chunkID);
        logger.debug("chunkID: " + chunkID + ", hash: " + hash);
        return getMasterReplicaNode(hash);
    }

    public KvNode getReplicaNode(String slotID) {
        KvNode node = mapSlotsNodes.get(slotID);
        if(node == null){
            logger.error("slot:" + slotID + " not found data node.");
            return null;
        }
        logger.debug("slot:" + slotID
                + ", host: " + node.host
                + ", port: " + node.port);
        return node;
    }

    public String getMasterReplicaSlotFromRGroup(TarimKVProto.RGroupItem rgroup){
        for(TarimKVProto.Slot slot : rgroup.getSlotsList()){
            if(slot.getRole() == TarimKVProto.SlotRole.SR_MASTER){
                logger.debug("rgroup:" + rgroup.getId() + "(hashValue:" + rgroup.getHashValue()
                        + "), master slot:" + slot.getId());
                return slot.getId();
            }
        }
        logger.error("rgroup:" + rgroup.getId() + "(hashValue:" + rgroup.getHashValue() + ") not found master slot.");
        return null;
    }
    public String getMasterReplicaSlotByHash(long hashValue) {
        if(dataDist == null) {
            refreshDistribution();
        }
        List<TarimKVProto.RGroupItem> rgroups = dataDist.getRgroupsList();
        if(rgroups.size() == 0){
            logger.error("fatal error, not found any rgroup, that means no data node.");
            return null;
        }else if(rgroups.size() == 1){
            return getMasterReplicaSlotFromRGroup(rgroups.get(0));
        }else{
            TarimKVProto.RGroupItem last;
            TarimKVProto.RGroupItem curr;
            for(int i = 1; i < rgroups.size(); i++)
            {
                last = rgroups.get(i-1);
                curr = rgroups.get(i);
                if(last.getHashValue() < hashValue && hashValue <= curr.getHashValue()){
                    return getMasterReplicaSlotFromRGroup(curr);
                }
            }
            return getMasterReplicaSlotFromRGroup(rgroups.get(0)); // 所有节点看成一个环，找不到的hash值即分布在第一个节点上。
        }
    }
    public String getMasterReplicaSlot(long chunkID) {
        long hash = MurmurHash3.hash32(chunkID);
        long unsignedHash = hash & 0x00000000FFFFFFFFL;
        logger.debug("chunkID: " + chunkID + ", hash: " + hash + ", unsigned:" + unsignedHash);
        return getMasterReplicaSlotByHash(unsignedHash);
    }

    public TarimProto.GetTableResponse loadTableRequest(String catalogName, String dataBaseName, String tableName){
        ManagedChannel channel;//客户端与服务器的通信channel
        TarimMetaGrpc.TarimMetaBlockingStub blockStub;
        channel = ManagedChannelBuilder.forAddress(metaHost, metaPort).usePlaintext().build();//指定grpc服务器地址和端口初始化通信channel
        blockStub = TarimMetaGrpc.newBlockingStub(channel);//根据通信channel初始化客户端存根节点

        TarimProto.GetTableRequest request = TarimProto.GetTableRequest.newBuilder()
                .setCatName(catalogName)
                .setDbName(dataBaseName)
                .setTblName(tableName)
                .build();
        TarimProto.GetTableResponse response = blockStub.getTable(request);
        channel.shutdown();
        return response;
    }

    public TarimProto.PrepareScanResponse preScan(int tableID, boolean allPartition, byte[] selections, List<String> columns, List<String> partitionIDs){
        ManagedChannel channel;//客户端与服务器的通信channel
        TarimMetaGrpc.TarimMetaBlockingStub blockStub;
        channel = ManagedChannelBuilder.forAddress(metaHost, metaPort).usePlaintext().build();//指定grpc服务器地址和端口初始化通信channel
        blockStub = TarimMetaGrpc.newBlockingStub(channel);//根据通信channel初始化客户端存根节点

        TarimExecutor.Executor.Builder executorBuilder = TarimExecutor.Executor.newBuilder();
        TarimExecutor.Selection selection = TarimExecutor.Selection.newBuilder().setConditions(ByteString.copyFrom(selections)).build();
        TarimExecutor.PartitionTableScan scan = TarimExecutor.PartitionTableScan.newBuilder()
                .setTableID(tableID)
                .build();

        TarimExecutor.Executor executor = executorBuilder
                .setExecType(TarimExecutor.ExecType.TypePartitionTableScan)
                .setSelection(selection)
                .setPartitionScan(scan)
                .build();

        TarimProto.PrepareScanRequest request = TarimProto.PrepareScanRequest.newBuilder()
                .setAllPartition(allPartition)
                .setTableID(tableID)
                .addExecutors(0, executor)
                .build();

        TarimProto.PrepareScanResponse response = blockStub.prepareScan(request);
        channel.shutdown();
        return response;

    }

    public TarimProto.DbStatusResponse partitionRequest(int tableID, String partitionID){
        ManagedChannel channel;//客户端与服务器的通信channel
        TarimMetaGrpc.TarimMetaBlockingStub blockStub;
        channel = ManagedChannelBuilder.forAddress(metaHost, metaPort).usePlaintext().build();//指定grpc服务器地址和端口初始化通信channel
        blockStub = TarimMetaGrpc.newBlockingStub(channel);//根据通信channel初始化客户端存根节点

        TarimProto.PartitionRequest request = TarimProto.PartitionRequest.newBuilder()
                .setTableID(tableID)
                .setPartitionID(partitionID)
                .build();

        TarimProto.DbStatusResponse response = blockStub.setPartition(request);
        channel.shutdown();
        return response;
    }
}
